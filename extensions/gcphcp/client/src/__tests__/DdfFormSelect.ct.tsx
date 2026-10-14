import { expect, test } from "@playwright/experimental-ct-react";

import {
  SelectBasic,
  SelectWithHelperText,
  SelectWithInitialValue,
  SelectWithRequired,
} from "./harnesses";

test.describe("DdfFormSelect", () => {
  test("renders label and all options", async ({ mount }) => {
    const component = await mount(<SelectBasic />);
    await expect(component.getByText("Favorite color")).toBeVisible();
    const select = component.locator("select");
    const options = select.locator("option");
    await expect(options).toHaveCount(3);
    await expect(options.nth(0)).toHaveText("Red");
    await expect(options.nth(1)).toHaveText("Blue");
    await expect(options.nth(2)).toHaveText("Green");
  });

  test("shows required indicator", async ({ mount }) => {
    const component = await mount(<SelectBasic />);
    const required = component.locator(".pf-v6-c-form__label-required");
    await expect(required).toBeVisible();
  });

  test("displays helper text", async ({ mount }) => {
    const component = await mount(<SelectWithHelperText />);
    await expect(component.getByText("Pick your favorite")).toBeVisible();
  });

  test("selects a value", async ({ mount }) => {
    const component = await mount(<SelectBasic />);
    const select = component.locator("select");
    await select.selectOption("blue");
    await expect(select).toHaveValue("blue");
  });

  test("shows validation error on blur when required and empty", async ({
    mount,
  }) => {
    const component = await mount(<SelectWithRequired />);
    const select = component.locator("select");
    await select.focus();
    await select.blur();
    await expect(
      component.locator(".pf-m-error .pf-v6-c-helper-text__item-text"),
    ).toBeVisible();
  });

  test("applies initial value", async ({ mount }) => {
    const component = await mount(<SelectWithInitialValue />);
    const select = component.locator("select");
    await expect(select).toHaveValue("blue");
  });
});
