import { expect, test } from "./fixtures";

// OME-257: basic accessibility checks on key screens. These are lightweight,
// dependency-free structural assertions (landmarks, headings, labelled
// controls) rather than a full axe audit. Cluster status and the delete
// confirmation dialog are covered functionally by the lifecycle journey.

test.describe("login screen", () => {
  // Empty storage state => logged out => bounced to the Dex login page.
  test.use({ storageState: { cookies: [], origins: [] } });

  test("form controls are labelled", async ({ page }) => {
    await page.goto("/");

    await expect(page.locator("h2")).toContainText("Log in to Your Account");
    await expect(
      page.getByRole("textbox", { name: "Email Address" }),
    ).toBeVisible();
    await expect(page.getByRole("textbox", { name: "Password" })).toBeVisible();

    // The submit control has a non-empty accessible name.
    const submit = page.getByRole("button");
    await expect(submit).toBeEnabled();
    expect((await submit.innerText()).trim().length).toBeGreaterThan(0);
  });
});

test.describe("authenticated console", () => {
  test.use({ storageState: ".auth/ops.json" });

  test("exposes navigation and main landmarks", async ({ page }) => {
    await page.goto("/core/clusters");

    await expect(page.getByRole("navigation")).toBeVisible();
    await expect(page.getByRole("main")).toBeVisible();
    await expect(
      page.getByRole("heading", { level: 1, name: "Clusters" }),
    ).toBeVisible();
    // The sidebar toggle carries an accessible name.
    await expect(
      page.getByRole("button", { name: "Navigation toggle" }),
    ).toBeVisible();
  });

  test("create cluster form fields are labelled", async ({ page }) => {
    await page.goto("/core/clusters?create=kind");

    const wizard = page.getByRole("dialog", { name: "Create cluster" });
    await expect(wizard.getByLabel("Cluster name")).toBeVisible();
    await expect(wizard.getByLabel("Node image")).toBeVisible();
  });
});
