const fs = require("node:fs");
const path = require("node:path");
const { test, expect } = require("@playwright/test");

const dryRunConfigPath = path.join(__dirname, "..", "..", "config", "dry-run.example.toml");
const copySettingsPath = path.join(__dirname, "..", "..", ".codex-longrun", "copy-ui-settings.json");
let originalDryRunConfig = null;
let originalCopySettings = null;

test.beforeEach(() => {
  originalDryRunConfig = fs.readFileSync(dryRunConfigPath, "utf8");
  originalCopySettings = fs.existsSync(copySettingsPath)
    ? fs.readFileSync(copySettingsPath, "utf8")
    : null;
  if (fs.existsSync(copySettingsPath)) {
    fs.rmSync(copySettingsPath);
  }
});

test.afterEach(() => {
  if (originalDryRunConfig != null) {
    fs.writeFileSync(dryRunConfigPath, originalDryRunConfig);
  }
  if (originalCopySettings != null) {
    fs.mkdirSync(path.dirname(copySettingsPath), { recursive: true });
    fs.writeFileSync(copySettingsPath, originalCopySettings);
  } else if (fs.existsSync(copySettingsPath)) {
    fs.rmSync(copySettingsPath);
  }
});

test("smart money page saves simple principal-capped copy settings", async ({ page }) => {
  await page.goto("/");
  await page.locator('button[data-view="copy"]').click();

  await expect(page.locator("#copy")).toHaveClass(/active/);
  await expect(page.locator("#copyPrincipalCap")).toHaveValue("10");
  await expect(page.locator("#copyLeverage")).toHaveValue("10");
  await expect(page.locator("#copyLeverage")).toHaveAttribute("min", "1");
  await expect(page.locator("#copyLeverage")).toHaveAttribute("max", "10");
  await expect(page.locator("#copyAccountPicker input")).toHaveCount(2);

  await page.locator("#copyClearAccounts").click();
  await page.locator('#copyForm button[type="submit"]').click();
  await expect(page.locator("#copyResult")).toContainText(/Select at least one local copy account|请至少选择一个本地跟单账号/);
  await page.locator("#copySelectAllAccounts").click();

  await page.locator("#copyLeaderAccounts").fill("not-a-wallet");
  await page.locator('#copyForm button[type="submit"]').click();
  await expect(page.locator("#copyResult")).toContainText(/Invalid target copy account|目标跟单账号格式不正确/);

  await page.locator("#copyLeaderAccounts").fill([
    "0x6d6d7c05ef7f31b31b618400495b4ce4092a5089",
    "0x6ac0b46b32dc429dbd129a503292f88649d2b8a0",
    "0x6d6d7c05ef7f31b31b618400495b4ce4092a5089",
  ].join("\n"));
  await page.locator("#copyRatio").fill("0.1");
  await page.locator("#copyPrincipalCap").fill("10");
  await page.locator("#copyLeverage").evaluate((node) => {
    node.value = "0.5";
  });

  await page.locator('#copyForm button[type="submit"]').click();

  const result = page.locator("#copyResult");
  await expect(result).toContainText("2");
  await expect(result).toContainText("$10.00");
  await expect(result).toContainText(/杠杆|leverage/);
  await expect(page.locator("#copyAddress")).toHaveValue("0x6d6d7c05ef7f31b31b618400495b4ce4092a5089");
  await expect(page.locator("#copyLeaderNotional")).toHaveValue("100");
  await expect(page.locator("#copyLeverage")).toHaveValue("10");

  const saved = await page.evaluate(() => JSON.parse(localStorage.getItem("trade_xyz_copy_simple_settings")));
  expect(saved.leaders).toEqual([
    "0x6d6d7c05ef7f31b31b618400495b4ce4092a5089",
    "0x6ac0b46b32dc429dbd129a503292f88649d2b8a0",
  ]);
  expect(saved.account_ids).toEqual(["addr_a", "addr_b"]);
  expect(saved.markets).toEqual(["xyz_perp", "hl_perp", "cash_perp", "spot"]);
  expect(saved.copy_ratio).toBe(0.1);
  expect(saved.principal_cap_usd).toBe(10);
});

test("smart money summary keeps copy-owned truth separate from other live positions", async ({ page }) => {
  await page.goto("/");
  await page.locator('button[data-view="copy"]').click();
  await expect(page.locator("#copy")).toHaveClass(/active/);

  await page.evaluate(() => {
    state.copySummary = {
      fetched_at_ms: Date.now(),
      leader_count: 2,
      leader_addresses: [
        "0x6d6d7c05ef7f31b31b618400495b4ce4092a5089",
        "0x6ac0b46b32dc429dbd129a503292f88649d2b8a0",
      ],
      account_ids: ["addr_a"],
      markets: ["xyz_perp", "hl_perp", "cash_perp", "spot"],
      shadow_signal_count: 3,
      rejected_count: 1,
      deduped_count: 0,
      copied_notional_usd: 125,
      submitted_reports: 1,
      order_evidence: 1,
      realized_pnl_usd: 0,
      unrealized_pnl_usd: 12.5,
      total_fees_usd: 0.5,
      pnl_status: "ok",
      live_running: true,
      live_round: 7,
      latest_signal_at_ms: Date.now(),
      pnl_report_stale: false,
      pnl_report_status: "fresh",
      local_summary: {
        id: "local_all",
        kind: "local_live_aggregate",
        truth_state: "current",
        total_pnl_usd: 20,
        unrealized_pnl_usd: 20,
        position_value_usd: 200,
        open_position_count: 2,
        positions: [
          {
            coin: "xyz:GOLD",
            side: "long",
            size: 0.02,
            position_value_usd: 125,
            unrealized_pnl_usd: 12.5,
            entry_px: 4000,
          },
          {
            coin: "xyz:MU",
            side: "short",
            size: 0.03,
            position_value_usd: 75,
            unrealized_pnl_usd: 7.5,
            entry_px: 1000,
          },
        ],
      },
      local_summaries: [
        {
          id: "addr_a",
          kind: "local_live",
          truth_state: "current",
          total_pnl_usd: 20,
          unrealized_pnl_usd: 20,
          position_value_usd: 200,
          open_position_count: 2,
          positions: [
            {
              coin: "xyz:GOLD",
              side: "long",
              size: 0.02,
              position_value_usd: 125,
              unrealized_pnl_usd: 12.5,
              entry_px: 4000,
            },
            {
              coin: "xyz:MU",
              side: "short",
              size: 0.03,
              position_value_usd: 75,
              unrealized_pnl_usd: 7.5,
              entry_px: 1000,
            },
          ],
        },
      ],
      copy_owned_summary: {
        id: "local_all",
        kind: "local_copy_owned",
        truth_state: "current",
        total_pnl_usd: 12.5,
        unrealized_pnl_usd: 12.5,
        position_value_usd: 125,
        open_position_count: 1,
        positions: [
          {
            coin: "xyz:GOLD",
            side: "long",
            size: 0.02,
            position_value_usd: 125,
            unrealized_pnl_usd: 12.5,
            entry_px: 4000,
          },
        ],
      },
      copy_owned_summaries: [
        {
          id: "addr_a",
          kind: "local_copy_owned_live",
          truth_state: "current",
          total_pnl_usd: 12.5,
          unrealized_pnl_usd: 12.5,
          position_value_usd: 125,
          open_position_count: 1,
          positions: [
            {
              coin: "xyz:GOLD",
              side: "long",
              size: 0.02,
              position_value_usd: 125,
              unrealized_pnl_usd: 12.5,
              entry_px: 4000,
            },
          ],
        },
      ],
      other_local_summary: {
        id: "local_all",
        kind: "local_non_copy_live",
        truth_state: "current",
        total_pnl_usd: 7.5,
        unrealized_pnl_usd: 7.5,
        position_value_usd: 75,
        open_position_count: 1,
        positions: [
          {
            coin: "xyz:MU",
            side: "short",
            size: 0.03,
            position_value_usd: 75,
            unrealized_pnl_usd: 7.5,
            entry_px: 1000,
          },
        ],
      },
      other_local_summaries: [
        {
          id: "addr_a",
          kind: "local_non_copy_live",
          truth_state: "current",
          total_pnl_usd: 7.5,
          unrealized_pnl_usd: 7.5,
          position_value_usd: 75,
          open_position_count: 1,
          positions: [
            {
              coin: "xyz:MU",
              side: "short",
              size: 0.03,
              position_value_usd: 75,
              unrealized_pnl_usd: 7.5,
              entry_px: 1000,
            },
          ],
        },
      ],
      leader_summaries: [],
      target_realized_pnl_usd: null,
      target_unrealized_pnl_usd: null,
      target_total_pnl_usd: null,
      target_position_value_usd: null,
      target_open_position_count: 0,
      target_position_state: "unavailable",
      latest_entries: [],
    };
    state.copySummaryAdvancedOpen = true;
    renderCopySummary();
  });

  const summary = page.locator("#copySummary");
  await expect(summary).toContainText(/跟单归属 1 条 \+ 未归属 1 条 = 选中账号实时总仓位 2 条|Copied 1 \+ unassigned 1 = total live 2/);
  await expect(summary.locator(".copy-position-row").first()).toContainText(/GOLD/i);
  await expect(summary.locator(".copy-position-row").first()).not.toContainText(/MU/i);

  const advanced = summary.locator(".copy-advanced-details");
  await expect(advanced).toHaveJSProperty("open", true);
  await expect(summary).toContainText(/未归属 \/ 其他策略持仓明细|Unassigned \/ other strategy position detail/);
  await expect(summary).toContainText(/MU/i);

  await page.evaluate(() => {
    renderCopySummary();
  });

  await expect(summary.locator(".copy-advanced-details")).toHaveJSProperty("open", true);
});

test("dashboard attribution consumes backend position truth fields", async ({ page }) => {
  await page.goto("/");

  const result = await page.evaluate(() => {
    state.copySummary = {
      copy_owned_summaries: [{
        id: "addr_a",
        positions: [{
          coin: "xyz:GOLD",
          side: "long",
          size: 1,
          position_value_usd: 100,
        }],
      }],
    };
    state.fibInstances = [{
      status: "protected",
      config: { coin: "xyz:GOLD", direction: "long" },
      pnl_summary: {
        open_position_size: 1,
        account_summaries: [{ account_id: "addr_a", open_position_size: 1 }],
      },
    }];

    const noBackend = dashboardPositionAttribution({
      account_id: "addr_a",
      coin: "xyz:GOLD",
      size: 1,
      position_value_usd: 100,
      pnl_usd: 10,
    });
    const backendMixed = dashboardPositionAttribution({
      account_id: "addr_a",
      coin: "xyz:GOLD",
      size: 1,
      position_value_usd: 100,
      pnl_usd: 10,
      owner: "mixed",
      attribution_source: "backend_position_truth",
      copy_ratio: 0.25,
      fib_ratio: 0.5,
      unattributed_ratio: 0.25,
      dust_ratio: 0,
      attribution_parts: [
        { key: "fib", source: "fib_strategy_order_oids", ratio: 0.5 },
        { key: "copy", source: "copy_ledger_live_position", ratio: 0.25 },
        { key: "unattributed", source: "live_position_without_strategy_evidence", ratio: 0.25 },
      ],
    });
    const backendDust = dashboardPositionAttribution({
      account_id: "addr_a",
      coin: "xyz:SILVER",
      size: 0.001,
      position_value_usd: 0.56,
      pnl_usd: 0,
      owner: "dust",
      attribution_source: "backend_position_truth",
      copy_ratio: 0,
      fib_ratio: 0,
      unattributed_ratio: 0,
      dust_ratio: 1,
      attribution_parts: [
        { key: "dust", source: "backend_dust_threshold", ratio: 1 },
      ],
    });
    const derivedDashboard = deriveDashboardView(
      [{ account_id: "addr_a" }, { account_id: "addr_b" }],
      {
        results: [
          {
            ok: true,
            data: {
              account_id: "addr_a",
              xyz_perp: {
                withdrawable_usd: 80,
                account_value_usd: 100,
                positions: [{
                  coin: "xyz:SP500",
                  size: 0.063,
                  entry_price: 7334.5,
                  position_value_usd: 462.0735,
                  unrealized_pnl_usd: -2.1,
                  owner: "copy",
                  attribution_source: "backend_position_truth",
                  copy_ratio: 1,
                  attribution_parts: [{ key: "copy", source: "copy_ledger_live_position", ratio: 1 }],
                }],
              },
            },
          },
          {
            ok: true,
            data: {
              account_id: "addr_b",
              xyz_perp: {
                withdrawable_usd: 25.747876,
                account_value_usd: 62.718611,
                positions: [{
                  coin: "xyz:SP500",
                  size: 0.018,
                  entry_price: 7334.5,
                  position_value_usd: 132.021,
                  unrealized_pnl_usd: -0.7,
                  owner: "copy",
                  attribution_source: "backend_position_truth",
                  copy_ratio: 1,
                  attribution_parts: [{ key: "copy", source: "copy_ledger_live_position", ratio: 1 }],
                }],
              },
            },
          },
        ],
      },
      "xyz_perp"
    );
    const rowPositionValue = derivedDashboard.positions.reduce(
      (sum, position) => sum + Number(position.position_value_usd || 0),
      0
    );

    return {
      noBackend,
      backendMixed,
      backendDust,
      dashboardTotalPositionValue: derivedDashboard.pnl.total_position_value_usd,
      dashboardRowPositionValue: rowPositionValue,
      dashboardLegacyMarginValue:
        derivedDashboard.pnl.total_equity_usd - derivedDashboard.pnl.total_available_usdc,
    };
  });

  expect(result.noBackend.owner).toBe("unattributed");
  expect(result.noBackend.copy_ratio).toBe(0);
  expect(result.noBackend.fib_ratio).toBe(0);
  expect(result.backendMixed.owner).toBe("mixed");
  expect(result.backendMixed.copy_ratio).toBe(0.25);
  expect(result.backendMixed.fib_ratio).toBe(0.5);
  expect(result.backendMixed.unattributed_ratio).toBe(0.25);
  expect(result.backendDust.owner).toBe("dust");
  expect(result.backendDust.dust_ratio).toBe(1);
  expect(result.dashboardTotalPositionValue).toBeCloseTo(594.0945, 4);
  expect(result.dashboardTotalPositionValue).toBeCloseTo(result.dashboardRowPositionValue, 6);
  expect(result.dashboardTotalPositionValue).not.toBeCloseTo(result.dashboardLegacyMarginValue, 4);
});

test("dashboard cancel-all action requires confirmation before calling the API", async ({ page }) => {
  await page.goto("/");

  const result = await page.evaluate(async () => {
    const originalApi = api;
    const originalConfirm = window.confirm;
    let apiCalled = false;
    try {
      api = async () => {
        apiCalled = true;
        throw new Error("cancel API should not be called after dismissed confirmation");
      };
      window.confirm = () => false;
      state.app = { ...(state.app || {}), dry_run: true };

      await dashboardCancelOpenOrdersAction();
      return {
        apiCalled,
        resultText: document.querySelector("#openOrdersResult")?.textContent || "",
      };
    } finally {
      api = originalApi;
      window.confirm = originalConfirm;
    }
  });

  expect(result.apiCalled).toBe(false);
  expect(result.resultText).not.toContain("Canceling");
  expect(result.resultText).not.toContain("正在撤销");
});
