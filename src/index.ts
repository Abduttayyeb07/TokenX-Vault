import "dotenv/config";
import { ethers } from "ethers";
import fs from "node:fs/promises";
import path from "node:path";

type ChainName = "ethereum" | "bsc";
type TokenName = "USDT" | "USDC";
type Wallet = { label: string; address: string };
type TokenConfig = { name: TokenName; address: string; decimals: number };
type ChainConfig = {
  name: ChainName;
  display: string;
  rpc: string[];
  ws: string[];
  native: string;
  tokens: TokenConfig[];
};
type CursorState = { lastBlock: number };
type State = { cursors: Record<string, CursorState> };
type Stats = {
  decoded: number;
  matched: number;
  scanned: number;
  alerts: number;
  lastWsBlock: number;
  lastHttpBlock: number;
  startedAt: number;
  lastDecodedAt: number;
  lastWsHeartbeatAt: number;
  lastWsError?: string;
};

const TRANSFER_TOPIC = ethers.id("Transfer(address,address,uint256)");
const ZERO_TOPIC = "0x" + "0".repeat(64);
const ERC20_ABI = [
  "function decimals() view returns (uint8)",
  "function balanceOf(address) view returns (uint256)",
];
const TOKENS: Record<ChainName, TokenConfig[]> = {
  ethereum: [
    {
      name: "USDT",
      address: "0xdAC17F958D2ee523a2206206994597C13D831ec7",
      decimals: 6,
    },
    {
      name: "USDC",
      address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
      decimals: 6,
    },
  ],
  bsc: [
    {
      name: "USDT",
      address: "0x55d398326f99059fF775485246999027B3197955",
      decimals: 18,
    },
    {
      name: "USDC",
      address: "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
      decimals: 18,
    },
  ],
};

const env = (key: string, fallback = "") =>
  process.env[key]?.trim() || fallback;
const intEnv = (key: string, fallback: number) =>
  Number.isFinite(Number(process.env[key]))
    ? Number(process.env[key])
    : fallback;
const boolEnv = (key: string, fallback: boolean) =>
  process.env[key] === undefined
    ? fallback
    : /^(1|true|yes)$/i.test(process.env[key]!);
const split = (value: string) =>
  value
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
const timeout = async <T>(
  promise: Promise<T>,
  ms: number,
  message: string,
): Promise<T> => {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), ms);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

const cfg = {
  telegramToken: env("TELEGRAM_BOT_TOKEN"),
  chatIds: split(env("TELEGRAM_CHAT_IDS")),
  healthIds: split(env("TELEGRAM_HEALTH_CHAT_IDS")),
  balanceIds: split(env("TELEGRAM_CHAT_IDS")),
  telegramTimeout: intEnv("TELEGRAM_TIMEOUT_MS", 20000),
  telegramRetries: intEnv("TELEGRAM_RETRIES", 3),
  commands: boolEnv("TELEGRAM_COMMANDS_ENABLED", true),
  pollMs: intEnv("POLL_INTERVAL_MS", 10000),
  confirmations: intEnv("CONFIRMATIONS", 1),
  maxRange: Math.max(1, intEnv("MAX_BLOCK_RANGE", 5)),
  maxBacklog: Math.max(1, intEnv("MAX_BACKLOG_BLOCKS", 25)),
  rpcTimeout: intEnv("RPC_TIMEOUT_MS", 20000),
  rpcDelay: intEnv("RPC_MIN_DELAY_MS", 400),
  stateFile: env("STATE_FILE", "./data/state.json"),
  ethBalanceRpc: env("ETH_BALANCE_RPC_URL"),
  minAlert: Number(process.env.MIN_ALERT_AMOUNT ?? 1),
  useWs: boolEnv("USE_WEBSOCKET", true),
  startLatest: boolEnv("START_FROM_LATEST_ON_BOOT", true),
  backfillOverlap: Math.max(0, intEnv("BACKFILL_OVERLAP_BLOCKS", 3)),
  incoming: boolEnv("ALERT_INCOMING", true),
  outgoing: boolEnv("ALERT_OUTGOING", true),
  health: boolEnv("TELEGRAM_HEALTH_UPDATE_ENABLED", true),
  healthMs: intEnv("TELEGRAM_HEALTH_UPDATE_INTERVAL_MS", 3600000),
  balanceTimes: split(env("BALANCE_REPORT_TIMES", "12:00,21:00")),
  balanceTimezone: env("BALANCE_REPORT_TIMEZONE", "Asia/Karachi"),
  wsSummary: boolEnv("LOG_WEBSOCKET_DECODED_SUMMARY", true),
  wsSummaryMs: intEnv("WEBSOCKET_DECODED_SUMMARY_INTERVAL_MS", 30000),
  tatumKey: env("TATUM_API_KEY"),
  stallMs: intEnv("WEBSOCKET_STALL_CHECK_INTERVAL_MS", 60000),
};

const wallets: Wallet[] = split(env("WATCHED_WALLETS")).map((item) => {
  const i = item.lastIndexOf("=");
  return {
    label: item.slice(0, i).trim(),
    address: ethers.getAddress(item.slice(i + 1).trim()),
  };
});
const chains: ChainConfig[] = [
  {
    name: "ethereum",
    display: "Ethereum",
    rpc: split(env("ETH_RPC_URLS")),
    ws: split(env("ETH_WS_URLS")),
    native: "ETH",
    tokens: TOKENS.ethereum,
  },
  {
    name: "bsc",
    display: "BNB Smart Chain",
    rpc: split(env("BSC_RPC_URLS")),
    ws: split(env("BSC_WS_URLS")),
    native: "BNB",
    tokens: TOKENS.bsc,
  },
];
const stats = new Map<string, Stats>();
const state: State = { cursors: {} };
const seenAlerts = new Set<string>();
const seenWsBillingAlerts = new Set<string>();
const backfillBusy = new Set<string>();
let lastTelegramUpdate = 0;

function key(chain: ChainName, token: TokenName) {
  return `${chain}:${token}`;
}
function newStats(): Stats {
  return {
    decoded: 0,
    matched: 0,
    scanned: 0,
    alerts: 0,
    lastWsBlock: 0,
    lastHttpBlock: 0,
    startedAt: Date.now(),
    lastDecodedAt: Date.now(),
    lastWsHeartbeatAt: 0,
  };
}
function topicsFor(wallet: string, position: 1 | 2) {
  const t = wallet.toLowerCase().replace(/^0x/, "").padStart(64, "0");
  const topics: (string | null)[] = [TRANSFER_TOPIC, null, null];
  topics[position] = `0x${t}`;
  return topics;
}
function parseLog(log: ethers.Log, token: TokenConfig) {
  const from = ethers.getAddress(`0x${log.topics[1].slice(-40)}`);
  const to = ethers.getAddress(`0x${log.topics[2].slice(-40)}`);
  const amount = BigInt(log.data);
  return {
    from,
    to,
    amount,
    formatted: ethers.formatUnits(amount, token.decimals),
    hash: log.transactionHash,
    block: log.blockNumber,
  };
}
function watched(address: string) {
  return wallets.find((w) => w.address.toLowerCase() === address.toLowerCase());
}

async function loadState() {
  try {
    Object.assign(state, JSON.parse(await fs.readFile(cfg.stateFile, "utf8")));
  } catch {
    /* first boot */
  }
}
async function saveState() {
  await fs.mkdir(path.dirname(cfg.stateFile), { recursive: true });
  await fs.writeFile(cfg.stateFile, JSON.stringify(state, null, 2));
}

async function telegram(chatIds: string[], text: string) {
  if (!cfg.telegramToken || !chatIds.length) return;
  for (const chatId of chatIds) {
    let last = "";
    for (let attempt = 1; attempt <= cfg.telegramRetries; attempt++) {
      try {
        const response = await timeout(
          fetch(
            `https://api.telegram.org/bot${cfg.telegramToken}/sendMessage`,
            {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                chat_id: chatId,
                text,
                disable_web_page_preview: true,
              }),
            },
          ),
          cfg.telegramTimeout,
          "Telegram request timed out",
        );
        if (!response.ok)
          throw new Error(
            `Telegram ${response.status} ${await response.text()}`,
          );
        break;
      } catch (error) {
        last = error instanceof Error ? error.message : String(error);
        if (attempt === cfg.telegramRetries)
          console.error(`Telegram send failed for ${chatId}: ${last}`);
        else await new Promise((r) => setTimeout(r, attempt * 1000));
      }
    }
  }
}

async function alertWsBillingIssue(
  chain: ChainConfig,
  token: TokenConfig,
  url: string,
  message: string,
) {
  if (
    !/(quota|payment|billing|subscription|plan|credit|unauthori[sz]ed)/i.test(
      message,
    )
  )
    return;
  const alertKey = `${chain.name}:${token.name}:${url}:${message}`;
  if (seenWsBillingAlerts.has(alertKey)) return;
  seenWsBillingAlerts.add(alertKey);
  await telegram(
    cfg.healthIds,
    `WebSocket provider billing/quota issue\n\nChain: ${chain.display}\nToken: ${token.name}\nEndpoint: ${url}\nMessage: ${message}\n\nTrying fallback WebSocket endpoint.`,
  );
}

async function rpcProvider(chain: ChainConfig) {
  let last = "";
  for (const url of chain.rpc) {
    try {
      const request = new ethers.FetchRequest(url);
      if (url.includes("tatum.io") && cfg.tatumKey)
        request.setHeader("x-api-key", cfg.tatumKey);
      const provider = new ethers.JsonRpcProvider(
        request,
        chain.name === "ethereum" ? 1 : 56,
        { staticNetwork: true, batchMaxCount: 1, pollingInterval: cfg.pollMs },
      );
      await timeout(
        provider.getBlockNumber(),
        cfg.rpcTimeout,
        `RPC timed out: ${url}`,
      );
      if (wallets[0])
        await timeout(
          provider.getBalance(wallets[0].address),
          cfg.rpcTimeout,
          `RPC balance check timed out: ${url}`,
        );
      console.log(`Connected ${chain.display} RPC: ${url}`);
      return { provider, url };
    } catch (error) {
      last = error instanceof Error ? error.message : String(error);
      console.error(`RPC failed ${url}: ${last}`);
    }
  }
  throw new Error(`No usable ${chain.display} RPC endpoint: ${last}`);
}

async function sendTransfer(
  chain: ChainConfig,
  token: TokenConfig,
  log: ethers.Log,
  stat: Stats,
  startup = false,
) {
  const event = parseLog(log, token);
  const destination = watched(event.to);
  const source = watched(event.from);
  const incoming = Boolean(destination);
  const wallet = destination ?? source;
  if (
    !wallet ||
    Number(event.formatted) < cfg.minAlert ||
    (incoming ? !cfg.incoming : !cfg.outgoing)
  )
    return;
  const eventId = `${chain.name}:${token.name}:${event.hash}:${log.index}`;
  if (seenAlerts.has(eventId)) return;
  seenAlerts.add(eventId);
  stat.matched++;
  stat.alerts++;
  let txFrom = event.from;
  try {
    const tx = await chainRuntime
      .get(chain.name)
      ?.provider.getTransaction(event.hash);
    if (tx?.from) txFrom = tx.from;
  } catch {
    /* event sender is sufficient */
  }
  const direction = incoming ? "Inflow" : "Outflow";
  const text = `${chain.display} ${token.name} ${direction}\n\nWallet: ${wallet.label}\nAmount: ${event.formatted} ${token.name}\nFrom: ${event.from}\nTx From: ${txFrom}\nTo: ${event.to}\nTx: ${event.hash}\nBlock: ${event.block}\nOpen: ${chain.name === "ethereum" ? "https://etherscan.io/tx/" : "https://bscscan.com/tx/"}${event.hash}`;
  console.log(
    `${startup ? "Startup " : ""}${direction} ${event.formatted} ${token.name} for ${wallet.label}: ${event.hash}`,
  );
  await telegram(cfg.chatIds, text);
}

type Runtime = {
  chain: ChainConfig;
  provider: ethers.JsonRpcProvider;
  url: string;
  rpcIndex?: number;
  ws?: ethers.WebSocketProvider;
  reconnecting?: boolean;
};
const chainRuntime = new Map<ChainName, Runtime>();

async function rotateRpc(runtime: Runtime) {
  if (runtime.chain.rpc.length < 2) return;
  const start =
    runtime.rpcIndex ?? Math.max(0, runtime.chain.rpc.indexOf(runtime.url));
  for (let offset = 1; offset <= runtime.chain.rpc.length; offset++) {
    const index = (start + offset) % runtime.chain.rpc.length;
    const url = runtime.chain.rpc[index];
    try {
      const request = new ethers.FetchRequest(url);
      if (url.includes("tatum.io") && cfg.tatumKey)
        request.setHeader("x-api-key", cfg.tatumKey);
      const provider = new ethers.JsonRpcProvider(
        request,
        runtime.chain.name === "ethereum" ? 1 : 56,
        { staticNetwork: true, batchMaxCount: 1, pollingInterval: cfg.pollMs },
      );
      await timeout(
        provider.getBlockNumber(),
        cfg.rpcTimeout,
        `RPC timed out: ${url}`,
      );
      if (wallets[0])
        await timeout(
          provider.getBalance(wallets[0].address),
          cfg.rpcTimeout,
          `RPC balance check timed out: ${url}`,
        );
      runtime.provider = provider;
      runtime.url = url;
      runtime.rpcIndex = index;
      console.warn(`Switched ${runtime.chain.display} HTTP RPC to ${url}`);
      return;
    } catch (error) {
      console.error(
        `RPC failover failed ${runtime.chain.display} ${url}: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  console.error(
    `No alternate usable ${runtime.chain.display} RPC endpoint found; will retry next cycle`,
  );
}

async function scanRange(
  runtime: Runtime,
  token: TokenConfig,
  from: number,
  to: number,
  startup = false,
) {
  const stat = stats.get(key(runtime.chain.name, token.name))!;
  if (to < from) return 0;
  const filterBase = { address: token.address, fromBlock: from, toBlock: to };
  const logs: ethers.Log[] = [];
  const seen = new Set<string>();
  for (const wallet of wallets)
    for (const topics of [
      topicsFor(wallet.address, 1),
      topicsFor(wallet.address, 2),
    ]) {
      try {
        const rows = await timeout(
          runtime.provider.getLogs({ ...filterBase, topics }),
          cfg.rpcTimeout,
          `eth_getLogs timed out for ${runtime.chain.name} ${token.name} ${from}-${to}`,
        );
        for (const row of rows)
          if (!seen.has(row.transactionHash + row.index)) {
            seen.add(row.transactionHash + row.index);
            logs.push(row);
          }
      } catch (error) {
        console.error(
          `Backfill failed ${runtime.chain.name} ${token.name} ${from}-${to}: ${error instanceof Error ? error.message : String(error)}`,
        );
        throw error;
      }
      await new Promise((r) => setTimeout(r, cfg.rpcDelay));
    }
  stat.scanned += to - from + 1;
  stat.lastHttpBlock = Math.max(stat.lastHttpBlock, to);
  if (logs.length)
    console.log(
      `HTTP backfill ${runtime.chain.name} ${token.name} ${from}-${to}: ${logs.length} matching log(s)`,
    );
  for (const log of logs)
    await sendTransfer(runtime.chain, token, log, stat, startup);
  return logs.length;
}

async function backfill(runtime: Runtime, token: TokenConfig) {
  const k = key(runtime.chain.name, token.name);
  const stat = stats.get(k)!;
  const latest = (await runtime.provider.getBlockNumber()) - cfg.confirmations;
  stat.lastHttpBlock = Math.max(stat.lastHttpBlock, latest);
  const cursor = Math.max(
    0,
    (state.cursors[k]?.lastBlock ??
      (cfg.startLatest ? latest : Math.max(0, latest - cfg.maxBacklog))) -
      cfg.backfillOverlap,
  );
  if (!state.cursors[k])
    console.log(
      `${runtime.chain.display} ${token.name} starting after block ${cursor}`,
    );
  let next = cursor + 1;
  let end = latest;
  if (end - next + 1 > cfg.maxBacklog) {
    console.warn(
      `${runtime.chain.display} ${token.name} backlog ${end - next + 1} exceeds ${cfg.maxBacklog}; skipping to latest window`,
    );
    next = Math.max(next, end - cfg.maxBacklog + 1);
  }
  while (next <= end) {
    const to = Math.min(end, next + cfg.maxRange - 1);
    console.log(
      `HTTP backfill ${runtime.chain.name} ${token.name} ${next}-${to}; latest=${latest}; backlog=${end - next + 1}`,
    );
    try {
      await scanRange(runtime, token, next, to);
      state.cursors[k] = { lastBlock: to };
      await saveState();
    } catch {
      console.error(
        `HTTP backfill will retry ${runtime.chain.name} ${token.name} from ${next}`,
      );
      break;
    }
    next = to + 1;
  }
}

async function runBackfill(runtime: Runtime, token: TokenConfig) {
  const k = key(runtime.chain.name, token.name);
  if (backfillBusy.has(k)) {
    console.warn(
      `HTTP backfill still running; skipping overlapping cycle for ${k}`,
    );
    return;
  }
  backfillBusy.add(k);
  try {
    await backfill(runtime, token);
  } catch (error) {
    console.error(
      `HTTP backfill cycle failed for ${k}: ${error instanceof Error ? error.message : String(error)}`,
    );
    await rotateRpc(runtime);
  } finally {
    backfillBusy.delete(k);
  }
}

async function startWs(runtime: Runtime, token: TokenConfig, wsUrlIndex = 0) {
  if (!cfg.useWs || !runtime.chain.ws.length) return;
  const url = runtime.chain.ws[wsUrlIndex % runtime.chain.ws.length];
  const stat = stats.get(key(runtime.chain.name, token.name))!;
  try {
    console.log(`Trying WebSocket ${runtime.chain.display}: ${url}`);
    const ws = new ethers.WebSocketProvider(
      url,
      runtime.chain.name === "ethereum" ? 1 : 56,
    );
    runtime.ws = ws;
    await timeout(
      ws.getBlockNumber(),
      cfg.rpcTimeout,
      `WebSocket timed out: ${url}`,
    );
    const filters = wallets
      .flatMap((wallet) => [
        topicsFor(wallet.address, 1),
        topicsFor(wallet.address, 2),
      ])
      .map((topics) => ({ address: token.address, topics }));
    console.log(
      `Subscribing watched-wallet ${runtime.chain.display} ${token.name} Transfer events on ${token.address} (${filters.length} filters)`,
    );
    for (const filter of filters)
      ws.on(filter, async (log) => {
        try {
          const item = parseLog(log as ethers.Log, token);
          stat.decoded++;
          stat.lastDecodedAt = Date.now();
          stat.lastWsBlock = Math.max(stat.lastWsBlock, item.block);
          const wallet = watched(item.from) ?? watched(item.to);
          if (wallet) {
            console.log(
              `WebSocket decoded matching ${runtime.chain.name} ${token.name} tx ${item.hash} block ${item.block} wallet=${wallet.label} direction=${watched(item.to) ? "inflow" : "outflow"}`,
            );
            await sendTransfer(runtime.chain, token, log as ethers.Log, stat);
          }
        } catch (error) {
          console.error(
            `WebSocket decode failed ${runtime.chain.name} ${token.name}: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      });
    runtime.ws.on("error", (error) => {
      stat.lastWsError = String(error);
      console.error(`WebSocket error ${runtime.chain.name}: ${String(error)}`);
      void alertWsBillingIssue(runtime.chain, token, url, stat.lastWsError);
      void reconnectWs(runtime, token, wsUrlIndex + 1);
    });
    const rawSocket = (
      ws as unknown as {
        websocket?: {
          on?: (event: string, callback: (...args: unknown[]) => void) => void;
        };
      }
    ).websocket;
    rawSocket?.on?.("close", (...args) => {
      stat.lastWsError = `socket closed ${args.map(String).join(" ")}`;
      console.error(
        `WebSocket closed ${runtime.chain.name} ${token.name}; reconnecting`,
      );
      void alertWsBillingIssue(runtime.chain, token, url, stat.lastWsError);
      void reconnectWs(runtime, token, wsUrlIndex + 1);
    });
    rawSocket?.on?.("error", (...args) => {
      stat.lastWsError = args.map(String).join(" ");
      console.error(
        `Underlying WebSocket error ${runtime.chain.name} ${token.name}: ${stat.lastWsError}`,
      );
    });
    console.log(
      `Subscribed to ${runtime.chain.display} ${token.name} Transfer events over WebSocket: ${url}`,
    );
    stat.lastWsHeartbeatAt = Date.now();
    stat.lastWsError = undefined;
    setInterval(() => {
      void checkWsHeartbeat(runtime, token, stat);
    }, cfg.stallMs);
  } catch (error) {
    stat.lastWsError = error instanceof Error ? error.message : String(error);
    console.error(
      `WebSocket failed ${runtime.chain.name} ${token.name}: ${stat.lastWsError}`,
    );
    void alertWsBillingIssue(runtime.chain, token, url, stat.lastWsError);
    setTimeout(() => void reconnectWs(runtime, token, wsUrlIndex + 1), 2000);
  }
}

async function reconnectWs(runtime: Runtime, token: TokenConfig, next = 0) {
  const stat = stats.get(key(runtime.chain.name, token.name));
  if (!stat?.lastWsError) return;
  if (stat && Date.now() - stat.lastWsHeartbeatAt < cfg.stallMs * 2) return;
  if (runtime.reconnecting) return;
  runtime.reconnecting = true;
  try {
    try {
      await runtime.ws?.destroy();
    } catch {
      /* closed already */
    }
    await new Promise((r) => setTimeout(r, 2000));
    await startWs(runtime, token, next);
  } finally {
    runtime.reconnecting = false;
  }
}

async function checkWsHeartbeat(
  runtime: Runtime,
  token: TokenConfig,
  stat: Stats,
) {
  if (!runtime.ws || runtime.reconnecting) return;
  try {
    const block = await timeout(
      runtime.ws.getBlockNumber(),
      cfg.rpcTimeout,
      `WebSocket heartbeat timed out: ${runtime.chain.name} ${token.name}`,
    );
    stat.lastWsHeartbeatAt = Date.now();
    if (block > stat.lastWsBlock) stat.lastWsBlock = block;
  } catch (error) {
    stat.lastWsError = error instanceof Error ? error.message : String(error);
    console.error(
      `WebSocket heartbeat failed ${runtime.chain.name} ${token.name}: ${stat.lastWsError}; reconnecting`,
    );
    void reconnectWs(runtime, token);
  }
}

async function balances(runtime: Runtime) {
  const parts = [`${runtime.chain.display}`];
  for (const wallet of wallets) {
    const nativeBalance = await runtime.provider.getBalance(wallet.address);
    parts.push(
      `\n${wallet.label}\n${runtime.chain.native.padEnd(5)} ${ethers.formatEther(nativeBalance)}`,
    );
    for (const token of runtime.chain.tokens) {
      const value = await new ethers.Contract(
        token.address,
        ERC20_ABI,
        runtime.provider,
      ).balanceOf(wallet.address);
      parts.push(
        `${token.name.padEnd(5)} ${ethers.formatUnits(value, token.decimals)}`,
      );
    }
  }
  return parts.join("\n");
}

function karachiDateTime() {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: cfg.balanceTimezone,
    year: "numeric",
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(new Date());
  return Object.fromEntries(
    parts.map((part) => [part.type, part.value]),
  ) as Record<string, string>;
}

const balanceProviders = new Map<ChainName, ethers.JsonRpcProvider>();

function balanceProviderFor(runtime: Runtime) {
  if (runtime.chain.name !== "ethereum" || !cfg.ethBalanceRpc)
    return runtime.provider;
  let provider = balanceProviders.get("ethereum");
  if (!provider) {
    provider = new ethers.JsonRpcProvider(cfg.ethBalanceRpc, 1, {
      staticNetwork: true,
      batchMaxCount: 1,
      pollingInterval: cfg.pollMs,
    });
    balanceProviders.set("ethereum", provider);
    console.log(
      `Ethereum balance queries using dedicated RPC: ${cfg.ethBalanceRpc}`,
    );
  }
  return provider;
}

async function balanceReport() {
  const now = karachiDateTime();
  const reports: string[] = [
    `📊 Balance Report — ${now.day}-${now.month}`,
    `Timezone: ${cfg.balanceTimezone}`,
  ];
  for (const chain of chains) {
    const runtime = chainRuntime.get(chain.name);
    reports.push(
      `\n━━━━━━━━━━━━━━━━━━━━\n${chain.name === "ethereum" ? "Ethereum" : "BNB Smart Chain"}`,
    );
    if (!runtime) {
      reports.push("Unavailable");
      continue;
    }
    const balanceProvider = balanceProviderFor(runtime);
    for (const wallet of wallets) {
      reports.push(`\n🏦 ${wallet.label}`);
      try {
        const native = await balanceProvider.getBalance(wallet.address);
        reports.push(`${chain.native.padEnd(6)} ${ethers.formatEther(native)}`);
      } catch (error) {
        console.error(
          `Native balance report query failed for ${chain.name} ${wallet.label}: ${error instanceof Error ? error.message : String(error)}`,
        );
        reports.push(`${chain.native.padEnd(6)} unavailable`);
      }
      for (const token of chain.tokens) {
        try {
          const value = await new ethers.Contract(
            token.address,
            ERC20_ABI,
            balanceProvider,
          ).balanceOf(wallet.address);
          reports.push(
            `${token.name.padEnd(6)} ${ethers.formatUnits(value, token.decimals)}`,
          );
        } catch (error) {
          console.error(
            `Token balance report query failed for ${chain.name} ${token.name} ${wallet.label}: ${error instanceof Error ? error.message : String(error)}`,
          );
          reports.push(`${token.name.padEnd(6)} unavailable`);
        }
      }
      reports.push(wallet.address);
    }
  }
  return reports.join("\n");
}

let lastBalanceReportKey = "";
function scheduleBalanceReports() {
  if (!cfg.balanceIds.length || !cfg.balanceTimes.length) return;
  setInterval(() => {
    const now = karachiDateTime();
    const hhmm = `${now.hour}:${now.minute}`;
    const reportKey = `${now.year}-${now.month}-${now.day}-${hhmm}`;
    if (cfg.balanceTimes.includes(hhmm) && reportKey !== lastBalanceReportKey) {
      lastBalanceReportKey = reportKey;
      void balanceReport()
        .then((report) => telegram(cfg.balanceIds, report))
        .catch((error) =>
          console.error(
            `Scheduled balance report failed: ${error instanceof Error ? error.message : String(error)}`,
          ),
        );
    }
  }, 30000);
}

function healthText() {
  const minutes = Math.max(
    1,
    Math.floor(
      (Date.now() - Math.min(...[...stats.values()].map((s) => s.startedAt))) /
        60000,
    ),
  );
  const lines = [
    `BSC + Ethereum Token Monitor Health`,
    `Window: ${Math.round(cfg.healthMs / 60000)} minute(s)`,
    `Uptime: ${minutes} minute(s)`,
  ];
  for (const chain of chains)
    for (const token of chain.tokens) {
      const s = stats.get(key(chain.name, token.name))!;
      lines.push(
        `\n${chain.display} ${token.name}\nWebSocket decoded: ${s.decoded}\nWebSocket matched: ${s.matched}\nHTTP backfill scanned: ${s.scanned} block(s)\nAlerts delivered: ${s.alerts}\nLast WS block: ${s.lastWsBlock || "none"}\nLast HTTP block: ${s.lastHttpBlock || "none"}${s.lastWsError ? `\nLast WS error: ${s.lastWsError}` : ""}`,
      );
    }
  return lines.join("\n");
}

async function commands() {
  if (!cfg.commands || !cfg.telegramToken) return;
  let offset = 0;
  while (true) {
    try {
      const response = await timeout(
        fetch(
          `https://api.telegram.org/bot${cfg.telegramToken}/getUpdates?timeout=20&offset=${offset}`,
        ),
        30000,
        "Telegram getUpdates timed out",
      );
      const data = (await response.json()) as {
        ok: boolean;
        result?: Array<{
          update_id: number;
          message?: { chat: { id: number }; text?: string };
        }>;
      };
      if (!data.ok) throw new Error(JSON.stringify(data));
      for (const update of data.result ?? []) {
        offset = update.update_id + 1;
        const text = update.message?.text?.trim() ?? "";
        const chat = String(update.message?.chat.id ?? "");
        if (!text.startsWith("/")) continue;
        try {
          if (text.startsWith("/balances"))
            await telegram([chat], await balanceReport());
          else if (text.startsWith("/status"))
            await telegram([chat], healthText());
          else if (text.startsWith("/blocks"))
            await telegram(
              [chat],
              [...stats.entries()]
                .map(
                  ([k, s]) =>
                    `${k}: HTTP=${s.lastHttpBlock || "none"}, WS=${s.lastWsBlock || "none"}, decoded=${s.decoded}`,
                )
                .join("\n"),
            );
          else if (text.startsWith("/verify"))
            await verify(chat, text.split(/\s+/).slice(1));
          else if (text.startsWith("/start") || text.startsWith("/help"))
            await telegram(
              [chat],
              "/balances\n/status\n/blocks\n/verify <chain> <token> <block> [toBlock]",
            );
        } catch (error) {
          await telegram(
            [chat],
            `Command failed: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }
    } catch (error) {
      console.error(
        `Telegram command poll failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

async function verify(chat: string, args: string[]) {
  const [chainName, tokenName, startText, endText] = args;
  const chain = chains.find(
    (c) => c.name === chainName || c.name.startsWith(chainName),
  );
  const token = chain?.tokens.find(
    (t) => t.name.toLowerCase() === tokenName?.toLowerCase(),
  );
  const start = Number(startText);
  const end = Number(endText ?? start);
  if (
    !chain ||
    !token ||
    !Number.isInteger(start) ||
    !Number.isInteger(end) ||
    end < start ||
    end < start ||
    end - start > 9
  )
    throw new Error(
      "Usage: /verify ethereum|bsc USDT|USDC <block> [toBlock], max 10 blocks",
    );
  const runtime = chainRuntime.get(chain.name)!;
  const rows = await runtime.provider.getLogs({
    address: token.address,
    fromBlock: start,
    toBlock: end,
    topics: [TRANSFER_TOPIC],
  });
  const matches = rows.filter((log) => {
    const e = parseLog(log, token);
    return watched(e.from) || watched(e.to);
  });
  const stat = stats.get(key(chain.name, token.name))!;
  for (const log of matches) await sendTransfer(chain, token, log, stat);
  await telegram(
    [chat],
    `Verification/replay ${chain.display} ${token.name} ${start}-${end}: ${matches.length} matching transfer(s)\n${
      matches
        .map((log) => {
          const e = parseLog(log, token);
          return `${e.formatted} ${token.name} ${watched(e.to) ? "inflow" : "outflow"} ${e.hash}`;
        })
        .join("\n") || "No watched-wallet transfer found."
    }`,
  );
}

async function main() {
  if (!wallets.length) throw new Error("WATCHED_WALLETS is empty");
  if (!cfg.telegramToken)
    console.warn("TELEGRAM_BOT_TOKEN is empty; alerts are disabled");
  await loadState();
  console.log(`Starting combined Ethereum + BSC USDT/USDC monitor`);
  console.log(
    `Wallets: ${wallets.map((w) => `${w.label}=${w.address}`).join(", ")}`,
  );
  for (const chain of chains) {
    if (!chain.rpc.length)
      throw new Error(`${chain.name.toUpperCase()}_RPC_URLS is empty`);
    const connected = await rpcProvider(chain);
    const runtime: Runtime = {
      chain,
      provider: connected.provider,
      url: connected.url,
    };
    chainRuntime.set(chain.name, runtime);
    for (const token of chain.tokens) {
      stats.set(key(chain.name, token.name), newStats());
      const stat = stats.get(key(chain.name, token.name))!;
      try {
        const decimals = await new ethers.Contract(
          token.address,
          ERC20_ABI,
          runtime.provider,
        ).decimals();
        token.decimals = Number(decimals);
      } catch {
        console.warn(
          `Could not load ${chain.name} ${token.name} decimals; using configured ${token.decimals}`,
        );
      }
      void runBackfill(runtime, token);
      void startWs(runtime, token);
      setInterval(() => {
        void runBackfill(runtime, token);
      }, cfg.pollMs);
      setInterval(() => {
        if (
          Date.now() - stat.lastDecodedAt > cfg.stallMs &&
          stat.lastHttpBlock > stat.lastWsBlock
        ) {
          console.warn(
            `WebSocket appears stalled for ${chain.name} ${token.name}; reconnecting`,
          );
          void reconnectWs(runtime, token);
        }
      }, cfg.stallMs);
      if (cfg.wsSummary)
        setInterval(
          () =>
            console.log(
              `WebSocket decoded ${stat.decoded} ${chain.name} ${token.name} Transfer event(s); matched watched wallets: ${stat.matched}; last block=${stat.lastWsBlock || "none"}`,
            ),
          cfg.wsSummaryMs,
        );
    }
  }
  if (cfg.health)
    setInterval(() => {
      void telegram(cfg.healthIds, healthText());
    }, cfg.healthMs);
  scheduleBalanceReports();
  if (cfg.commands) void commands();
  await telegram(
    cfg.chatIds,
    `Monitor started\nChains: Ethereum + BNB Smart Chain\nTokens: USDT + USDC\nWallets: ${wallets.map((w) => w.label).join(", ")}`,
  );
}

process.on("unhandledRejection", (reason) => {
  console.error(
    `Unhandled promise rejection: ${reason instanceof Error ? (reason.stack ?? reason.message) : String(reason)}`,
  );
});
process.on("uncaughtException", (error) => {
  console.error(`Uncaught exception: ${error.stack ?? error.message}`);
  console.error(
    "Monitor kept alive; inspect this error and the affected RPC/WebSocket connection.",
  );
});

main().catch((error) => {
  console.error(
    `Fatal startup error: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`,
  );
  process.exitCode = 1;
});
