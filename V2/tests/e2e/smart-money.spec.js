const { test, expect } = require("@playwright/test");

test("smart money page saves simple principal-capped copy settings", async ({ page }) => {
  await page.goto("/");
  await page.locator('button[data-view="copy"]').click();

  await expect(page.locator("#copy")).toHaveClass(/active/);
  await expect(page.locator("#copyPrincipalCap")).toHaveValue("10");
  await expect(page.locator("#copyLeverage")).toHaveValue("5");
  await expect(page.locator("#copyLeverage")).toHaveAttribute("min", "1");
  await expect(page.locator("#copyLeverage")).toHaveAttribute("max", "5");

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
  await expect(page.locator("#copyLeverage")).toHaveValue("5");

  const saved = await page.evaluate(() => JSON.parse(localStorage.getItem("trade_xyz_copy_simple_settings")));
  expect(saved.leaders).toEqual([
    "0x6d6d7c05ef7f31b31b618400495b4ce4092a5089",
    "0x6ac0b46b32dc429dbd129a503292f88649d2b8a0",
  ]);
  expect(saved.copy_ratio).toBe(0.1);
  expect(saved.principal_cap_usd).toBe(10);
});
