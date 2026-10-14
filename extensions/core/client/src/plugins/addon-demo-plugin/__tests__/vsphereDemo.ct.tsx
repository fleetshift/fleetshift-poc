import { expect, test } from "@playwright/experimental-ct-react";

import { VsphereDemoHarness } from "./VsphereDemoHarness";

test("vSphere wizard explains its demo status and closes without provisioning", async ({
  mount,
  page,
}) => {
  const requests: string[] = [];
  page.on("request", (request) => {
    if (request.method() === "POST") requests.push(request.url());
  });
  const component = await mount(<VsphereDemoHarness />);
  await expect(component).toContainText("Demonstration only");
  await component
    .getByRole("button", { name: "Select vSphere provider" })
    .click();
  await expect(component).toContainText("Provisioning is not implemented");
  await component.getByRole("button", { name: "Next", exact: true }).click();
  await component.getByRole("button", { name: "Next", exact: true }).click();
  await expect(component).toContainText("does not create a cluster");
  await component.getByRole("button", { name: "Close demo" }).click();
  await expect(component).toContainText("Demo finished");
  expect(requests).toEqual([]);
});

test("vSphere onboarding explains that completing the demo creates no connection", async ({
  mount,
}) => {
  const component = await mount(<VsphereDemoHarness onboarding />);
  await expect(component).toContainText("no connection is created");
  await component.getByRole("button", { name: "Configure" }).click();
  await expect(component).toContainText("No connection is created");
  await component.getByRole("button", { name: "Complete" }).click();
  await expect(component).toContainText("Demo finished");
});
