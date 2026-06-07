# trade.xyz V2

V2 is the next architecture for the trade.xyz automated trading system.

The V1 system proved the exchange integration, frontend controls, three-market
support, Fib Basic strategy, native TP/SL, and WebSocket post fast submit path.
V2 keeps those lessons but changes the execution model:

- the frontend no longer sits on the critical trading path
- strategies and copy-trading modules emit internal signals
- per-account workers keep signer, nonce, realtime state, and risk context warm
- order submission happens through a direct internal worker channel

Start with `docs/README.md`.

The Rust package name is `trade_xyz_bot_v2` so V2 build outputs and process
names are easy to distinguish from V1.

