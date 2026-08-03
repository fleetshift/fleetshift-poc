import { expect, test } from "@playwright/experimental-ct-react";

import { ClusterDetailsStep1 } from "./harnesses";

test.describe("Cluster details step 1 schema", () => {
  test("renders all four fields", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    await expect(component.getByText("Cluster ID")).toBeVisible();
    await expect(component.getByText("Endpoint access")).toBeVisible();
    await expect(component.getByText("Release version")).toBeVisible();
    await expect(component.getByText("Channel group")).toBeVisible();
  });

  test("cluster ID shows helper text", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    await expect(
      component.getByText(
        "Lowercase letters, digits, and hyphens. Max 15 characters.",
      ),
    ).toBeVisible();
  });

  test("cluster ID shows placeholder", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const input = component.getByPlaceholder("my-hcp-cluster");
    await expect(input).toBeVisible();
  });

  test("endpoint access has correct default", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const select = component.locator("#endpointAccess");
    await expect(select).toHaveValue("PublicAndPrivate");
  });

  test("endpoint access shows all options", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const select = component.locator("#endpointAccess");
    const options = select.locator("option");
    await expect(options).toHaveCount(3);
  });

  test("channel group defaults to stable", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const select = component.locator("#channelGroup");
    await expect(select).toHaveValue("stable");
  });

  test("channel group shows all four options", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const select = component.locator("#channelGroup");
    const options = select.locator("option");
    await expect(options).toHaveCount(4);
  });

  test("cluster ID rejects uppercase", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const input = component.getByPlaceholder("my-hcp-cluster");
    await input.fill("MyCluster");
    await input.blur();
    await expect(
      component.getByText("Must start with a lowercase letter"),
    ).toBeVisible();
  });

  test("cluster ID rejects over 15 chars", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const input = component.getByPlaceholder("my-hcp-cluster");
    await input.fill("abcdefghijklmnop");
    await input.blur();
    await expect(
      component.getByText(/Must be 15 characters or less/),
    ).toBeVisible();
  });

  test("cluster ID accepts valid input", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const input = component.getByPlaceholder("my-hcp-cluster");
    await input.fill("my-cluster-1");
    await input.blur();
    await expect(
      component.getByText("Must start with a lowercase letter"),
    ).toBeHidden();
    await expect(
      component.getByText(/Must be 15 characters or less/),
    ).toBeHidden();
  });

  test("can change endpoint access", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const select = component.locator("#endpointAccess");
    await select.selectOption("Private");
    await expect(select).toHaveValue("Private");
  });

  test("can change channel group", async ({ mount }) => {
    const component = await mount(<ClusterDetailsStep1 />);
    const select = component.locator("#channelGroup");
    await select.selectOption("fast");
    await expect(select).toHaveValue("fast");
  });
});
