# 文档总览

这套文档是后续落代码的共同契约。开发时优先遵守 `AGENTS.md` 和本目录文档；
如果实现中发现文档和真实 API 或实盘行为不一致，先更新文档，再改代码。

## 阅读顺序

1. [系统架构](architecture.md)
2. [产品需求说明](product-requirements.md)
3. [技术栈](tech-stack.md)
4. [多进程与多地址执行模型](process-model.md)
5. [内部 API 契约](internal-apis.md)
6. [风控模型](risk-model.md)
7. [策略开发指南](strategy-development.md)
8. [斐波那契回撤策略开发文档](fibonacci-retracement-development.md)
9. [斐波那契基础版多空方向扩展开发文档](fibonacci-long-short-extension.md)
10. [斐波那契基础版前端体验开发文档](fibonacci-basic-ux.md)
11. [低延迟执行 fast path](low-latency-execution.md)
12. [前端控制台](frontend-console.md)
13. [基本操作模块](manual-operations.md)
14. [配置规范](configuration.md)
15. [运行手册](operations.md)
16. [测试策略](testing.md)
17. [安全与密钥](security-and-secrets.md)
18. [实施路线图](implementation-roadmap.md)
19. [官方文档合规审计 2026-06-03](official-compliance-audit-2026-06-03.md)
20. [斐波那契基础版模块审计 2026-06-03](fib-basic-module-audit-2026-06-03.md)

## 设计目标

- 用 Rust 构建长期运行、低延迟、可恢复的自动交易系统。
- 支持 trade[XYZ] 的 HIP-3 `xyz` perp DEX，同时保留扩展到其他 Hyperliquid
  市场或其他策略的能力。
- 把“信息获取、策略决策、风控裁决、交易执行、状态存储”分开。
- 让斐波那契策略、聪明钱跟单策略和未来策略共用基础设施，但不互相污染。

## 模块总览

```text
基础信息模块 -> 信号协调层 -> 每地址 account worker -> 风控网关 -> 基础交易模块
                     |              |                 |            |
                     v              v                 v            v
                  状态存储       地址状态          风控状态      订单状态
```

## 必须遵守的边界

- 策略模块不得直接调用交易所下单 API。
- 基本操作模块不得直接调用交易所下单 API。
- 每个本地交易地址必须由独立 account worker 进程执行。
- 基础交易模块不得知道策略细节。
- 风控网关是所有下单意图进入执行层的唯一入口。
- 状态存储必须记录足够信息用于重启恢复、对账、去重、审计。
- 任何主网实盘功能必须显式配置 `environment = "mainnet"` 且 `dry_run = false`。
