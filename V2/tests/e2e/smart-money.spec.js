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
