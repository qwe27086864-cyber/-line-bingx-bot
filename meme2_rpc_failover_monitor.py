"""
MEME2 monitor RPC failover wrapper.

Transport-only patch:
- keeps meme2_wallet_copy_v2.py strategy logic unchanged
- replaces only the Solana RPC transport
- quickly fails over between available endpoints
- never logs endpoint URLs or API keys
- PAPER ONLY, same as the wrapped strategy
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

import meme2_wallet_copy_v2 as v3


RPC_TIMEOUT_SEC = float(os.environ.get("MEME2_RPC_TIMEOUT_SEC", "6"))
RPC_FAILOVER_ROUNDS = max(
    1, int(os.environ.get("MEME2_RPC_FAILOVER_ROUNDS", "1"))
)
RPC_FAILOVER_PAUSE_SEC = float(
    os.environ.get("MEME2_RPC_FAILOVER_PAUSE_SEC", "1.0")
)


def build_rpc_endpoints():
    endpoints = []

    helius_key = os.environ.get("HELIUS_API_KEY", "").strip()
    if helius_key:
        endpoints.append(
            f"https://mainnet.helius-rpc.com/?api-key={helius_key}"
        )

    primary = (
        os.environ.get("MEME2_RPC_HTTP", "").strip()
        or str(v3.core.SOLANA_RPC or "").strip()
    )
    if primary:
        endpoints.append(primary)

    fallback = os.environ.get(
        "MEME2_RPC_FALLBACK_HTTP",
        "https://api.mainnet-beta.solana.com",
    ).strip()
    if fallback:
        endpoints.append(fallback)

    return list(dict.fromkeys(x for x in endpoints if x))


RPC_ENDPOINTS = build_rpc_endpoints()


def failover_rpc(method, params):
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
    ).encode("utf-8")

    if not RPC_ENDPOINTS:
        raise RuntimeError("MEME2 RPC endpoints are not configured")

    last_exc = None

    for round_no in range(RPC_FAILOVER_ROUNDS):
        for endpoint_no, endpoint in enumerate(RPC_ENDPOINTS, start=1):
            v3._wait_slot("rpc")

            req = urllib.request.Request(
                endpoint,
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": v3.core.USER_AGENT,
                },
            )

            try:
                with urllib.request.urlopen(
                    req,
                    timeout=RPC_TIMEOUT_SEC,
                ) as resp:
                    payload = json.loads(
                        resp.read().decode("utf-8")
                    )

                if payload.get("error"):
                    raise RuntimeError(
                        f"RPC {method} returned an RPC error"
                    )

                if endpoint_no > 1:
                    print(
                        "MEME2 RPC failover success "
                        f"method={method} "
                        f"endpoint={endpoint_no}/{len(RPC_ENDPOINTS)}",
                        flush=True,
                    )

                return payload.get("result")

            except urllib.error.HTTPError as exc:
                last_exc = exc
                print(
                    "MEME2 RPC endpoint failed "
                    f"method={method} "
                    f"endpoint={endpoint_no}/{len(RPC_ENDPOINTS)} "
                    f"type=HTTP{exc.code}",
                    flush=True,
                )

            except Exception as exc:
                last_exc = exc
                print(
                    "MEME2 RPC endpoint failed "
                    f"method={method} "
                    f"endpoint={endpoint_no}/{len(RPC_ENDPOINTS)} "
                    f"type={type(exc).__name__}",
                    flush=True,
                )

        if round_no + 1 < RPC_FAILOVER_ROUNDS:
            time.sleep(max(0.0, RPC_FAILOVER_PAUSE_SEC))

    print(
        "MEME2 RPC exhausted failover "
        f"method={method} "
        f"endpoints={len(RPC_ENDPOINTS)} "
        f"rounds={RPC_FAILOVER_ROUNDS}",
        flush=True,
    )

    raise RuntimeError(
        "RPC failed after failover: "
        f"{type(last_exc).__name__ if last_exc else 'unknown'}"
    )


v3.core.rpc = failover_rpc


def main():
    print(
        "MEME2_RPC_FAILOVER_PATCH "
        f"endpoints={len(RPC_ENDPOINTS)} "
        f"timeout={RPC_TIMEOUT_SEC:.1f}s "
        f"rounds={RPC_FAILOVER_ROUNDS} "
        "strategy_unchanged=true",
        flush=True,
    )

    mode = os.environ.get("MEME2_MODE", "monitor").strip().lower()
    if "--monitor" in sys.argv or mode == "monitor":
        v3.monitor_loop()
    else:
        v3.run_once()


if __name__ == "__main__":
    main()
