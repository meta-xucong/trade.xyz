const { test, expect } = require("@playwright/test");

test("smart money page previews principal-capped leverage sizing", async ({ page }) => {
  await page.goto("/");
  await page.locator('button[data-view="copy"]').click();

  await expect(page.locator("#copy")).toHaveClass(/active/);
  await expect(page.locator("#copyPrincipalCap")).toHaveValue("10");
  await expect(page.locator("#copyLeverage")).toHaveValue("5");
  await expect(page.locator("#copyLeverage")).toHaveAttribute("min", "1");

  await page.locator("#copyLeaderNotional").fill("1000");
  await page.locator("#copyRatio").fill("0.1");
  await page.locator("#copyPrincipalCap").fill("10");
  await page.locator("#copyLeverage").fill("5");

  const previewResponsePromise = page.waitForResponse((response) => (
    response.url().includes("/api/smart-money/preview")
      && response.request().method() === "POST"
  ));
  await page.locator('#copyForm button[type="submit"]').click();
  const previewResponse = await previewResponsePromise;
  const preview = await previewResponse.json();

  const result = page.locator("#copyResult");
  await expect(result).toContainText("copied_notional_usd: 50");
  expect(preview.ok).toBe(true);
  expect(preview.data.copied_notional_usd).toBe(50);
  expect(preview.data.sizing.capped_principal_usd).toBe(10);
  expect(preview.data.sizing.max_leverage).toBe(5);

  await page.locator("#copyLeverage").fill("0.5");
  await expect.poll(() =>
    page.locator("#copyLeverage").evaluate((node) => node.validity.rangeUnderflow)
  ).toBe(true);
  await expect.poll(() =>
    page.locator("#copyLeverage").evaluate((node) => node.validationMessage.length > 0)
  ).toBe(true);
  await page.locator('#copyForm button[type="submit"]').click();
  await expect(result).toContainText("copied_notional_usd: 50");
});
