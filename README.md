# trade.xyz Automated Trading System

This repository is now split into versioned workspaces:

- `V1/`: current accepted Rust implementation with the manual console, Fib
  Basic, three-market support, WebSocket-first read cache, and low-latency
  WebSocket post fast submit path.
- `V2/`: next design track for a lower-latency architecture based on warm
  per-account workers and internal signal channels.

V1 and V2 are intentionally siblings so V2 can be designed and built without
destabilizing the accepted V1 codebase.

