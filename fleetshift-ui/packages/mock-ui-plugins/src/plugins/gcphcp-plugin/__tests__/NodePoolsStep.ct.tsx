import { expect, test } from "@playwright/experimental-ct-react";

import { NodePoolsDefault, NodePoolsTwoPools } from "./harnesses";

test.describe("NodePoolsStep", () => {
  test("renders all pool fields from config", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    await expect(component.getByText("Pool ID")).toBeVisible();
    await expect(component.getByText("Replicas")).toBeVisible();
    await expect(component.getByText("Instance type")).toBeVisible();
    await expect(component.getByText("Root volume size (GB)")).toBeVisible();
    await expect(component.getByText("Root volume type")).toBeVisible();
    await expect(component.getByText("Upgrade type")).toBeVisible();
    await expect(component.getByText("Enable auto-repair")).toBeVisible();
  });

  test("renders default values from DEFAULT_NODEPOOL", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    const replicas = component.locator("#replicas-0 input");
    await expect(replicas).toHaveValue("2");
    const instanceType = component.locator("#instanceType-0");
    await expect(instanceType).toHaveValue("n1-standard-4");
    const autoRepair = component.locator("#autoRepair-0");
    await expect(autoRepair).toBeChecked();
  });

  test("updates pool ID via text input", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    const poolId = component.locator("#id-0");
    await poolId.fill("workers");
    await expect(poolId).toHaveValue("workers");
  });

  test("pool ID shows error for invalid pattern", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    const poolId = component.locator("#id-0");
    await poolId.fill("123-bad");
    await expect(poolId).toHaveAttribute("aria-invalid", "true");
  });

  test("pool ID accepts valid input", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    const poolId = component.locator("#id-0");
    await poolId.fill("my-pool");
    await expect(poolId).not.toHaveAttribute("aria-invalid", "true");
  });

  test("selects instance type", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    const select = component.locator("#instanceType-0");
    await select.selectOption("n2-standard-8");
    await expect(select).toHaveValue("n2-standard-8");
  });

  test("toggles auto-repair checkbox", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    const checkbox = component.locator("#autoRepair-0");
    await expect(checkbox).toBeChecked();
    await checkbox.uncheck();
    await expect(checkbox).not.toBeChecked();
  });

  test("adds a second node pool", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    await component.getByRole("button", { name: "Add node pool" }).click();
    await expect(component.getByText("Node pool 2")).toBeVisible();
  });

  test("removes a node pool", async ({ mount }) => {
    const component = await mount(<NodePoolsTwoPools />);
    await expect(component.getByText("pool-a")).toBeVisible();
    await expect(component.getByText("pool-b")).toBeVisible();
    const removeButtons = component.getByLabel("Remove node pool");
    await removeButtons.first().click();
    await expect(component.getByText("pool-a")).toBeHidden();
    await expect(component.getByText("pool-b")).toBeVisible();
  });

  test("cannot remove last pool", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    const removeButton = component.getByLabel("Remove node pool");
    await expect(removeButton).toBeDisabled();
  });

  test("pool summary shows in header", async ({ mount }) => {
    const component = await mount(<NodePoolsDefault />);
    await expect(
      component.getByText("2x n1-standard-4, 128GB pd-standard"),
    ).toBeVisible();
  });
});
