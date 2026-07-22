import { expect, type Page, test } from "@playwright/experimental-ct-react";

import AdvancedSearchBar from "../AdvancedSearchBar";

function noop() {}

interface SeedEntry {
  expression: string;
  timestamp: number;
  favorite: boolean;
}

async function seedHistory(page: Page, entries: SeedEntry[]) {
  await page.evaluate((items) => {
    return new Promise<void>((resolve, reject) => {
      const req = indexedDB.open("fleetshift-search-history", 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains("history"))
          db.createObjectStore("history");
      };
      req.onsuccess = () => {
        const db = req.result;
        const tx = db.transaction("history", "readwrite");
        const store = tx.objectStore("history");
        for (const item of items) store.put(item, item.expression);
        tx.oncomplete = () => {
          db.close();
          resolve();
        };
        tx.onerror = () => reject(tx.error);
      };
      req.onerror = () => reject(req.error);
    });
  }, entries);
}

async function clearHistory(page: Page) {
  await page.evaluate(() => {
    return new Promise<void>((resolve) => {
      const req = indexedDB.deleteDatabase("fleetshift-search-history");
      req.onsuccess = () => resolve();
      req.onerror = () => resolve();
      req.onblocked = () => resolve();
    });
  });
}

test.describe("Advanced CEL search", () => {
  test("shows top-level fields on mount", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const menu = component.locator(".ome-search__autocomplete");
    await expect(menu).toBeVisible();
    await expect(
      menu.getByRole("menuitem", { name: /^Resource Name/ }),
    ).toBeVisible();
    await expect(
      menu.getByRole("menuitem", { name: /^Resource Type/ }),
    ).toBeVisible();
    await expect(
      menu.getByRole("menuitem", { name: /^Resource Resource data/ }),
    ).toBeVisible();
  });

  test("path walking — resource. shows children", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill("resource.");
    const menu = component.locator(".ome-search__autocomplete");
    await expect(
      menu.getByRole("menuitem", { name: /^Conditions/ }),
    ).toBeVisible();
    await expect(
      menu.getByRole("menuitem", { name: /^Observation/ }),
    ).toBeVisible();
  });

  test("semantic did you mean — typing cluster", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill("cluster");
    const menu = component.locator(".ome-search__autocomplete");
    await expect(menu.getByText("Did you mean?")).toBeVisible();
  });

  test("accepts suggestion with Tab", async ({ mount, page }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill("resource.");
    await page.keyboard.press("Tab");
    const value = await input.inputValue();
    expect(value.length).toBeGreaterThan("resource.".length);
  });

  test("enum values after operator", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill("resource.observation.kind == ");
    const menu = component.locator(".ome-search__autocomplete");
    await expect(menu.getByText("Pod")).toBeVisible();
    await expect(menu.getByText("Deployment")).toBeVisible();
  });

  test("CEL preview shows at top of menu as valid", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill('resource.observation.kind == "Pod"');
    const preview = component.locator(".ome-search__cel-preview-header");
    await expect(preview).toBeVisible();
    await expect(
      preview.locator(".ome-search__cel-preview-icon--valid"),
    ).toBeVisible();
  });

  test("CEL preview shows validation error", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill('name == "test');
    // Wait for debounced validation (300ms debounce + margin)
    await component.page().waitForTimeout(500);
    const preview = component.locator(".ome-search__cel-preview-header");
    await expect(preview).toBeVisible();
    await expect(
      preview.locator(".ome-search__cel-preview-icon--invalid"),
    ).toBeVisible();
  });

  test("condition path walking — Ready shows status/reason", async ({
    mount,
  }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill("resource.conditions.Ready.");
    const menu = component.locator(".ome-search__autocomplete");
    await expect(menu.getByText("Status")).toBeVisible();
    await expect(menu.getByText("Reason")).toBeVisible();
    await expect(menu.getByText("Message")).toBeVisible();
  });

  test("placeholder text rotates", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    const first = await input.getAttribute("placeholder");
    expect(first).toContain("Try:");
    await component.page().waitForTimeout(4500);
    const second = await input.getAttribute("placeholder");
    expect(second).toContain("Try:");
    expect(second).not.toBe(first);
  });

  test("Escape closes menu, second Escape deactivates", async ({
    mount,
    page,
  }) => {
    let deactivated = false;
    const component = await mount(
      <AdvancedSearchBar
        onDeactivate={() => {
          deactivated = true;
        }}
        onExecute={noop}
      />,
    );
    const menu = component.locator(".ome-search__autocomplete");
    await expect(menu).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(menu).toBeHidden();
    await page.keyboard.press("Escape");
    expect(deactivated).toBe(true);
  });

  test("Enter executes the expression", async ({ mount, page }) => {
    let executed = "";
    const component = await mount(
      <AdvancedSearchBar
        onDeactivate={noop}
        onExecute={(expr) => {
          executed = expr;
        }}
      />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill('name == "test"');
    await page.keyboard.press("Enter");
    expect(executed).toBe('name == "test"');
  });

  test("clicking autocomplete suggestion keeps menu open", async ({
    mount,
  }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill("resource.");
    const menu = component.locator(".ome-search__autocomplete");
    await expect(menu).toBeVisible();
    await menu.getByRole("menuitem", { name: /^Observation/ }).click();
    await expect(menu).toBeVisible();
    const value = await input.inputValue();
    expect(value).toBe("resource.observation.");
  });

  test("clicking operator chip keeps menu open", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill("resource.");
    const menu = component.locator(".ome-search__autocomplete");
    await menu.getByRole("menuitem", { name: /^Observation/ }).click();
    await menu.getByRole("menuitem", { name: /^Kind/ }).click();
    await menu.getByRole("button", { name: "==" }).click();
    await expect(menu).toBeVisible();
    const value = await input.inputValue();
    expect(value).toContain("==");
  });

  test("clicking value suggestion keeps menu open", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill("resource.observation.kind == ");
    const menu = component.locator(".ome-search__autocomplete");
    await menu.getByRole("menuitem", { name: "Pod" }).click();
    await expect(menu).toBeVisible();
    const value = await input.inputValue();
    expect(value).toContain('"Pod"');
  });

  test("Enter executes but keeps menu open for results", async ({
    mount,
    page,
  }) => {
    let executed = "";
    const component = await mount(
      <AdvancedSearchBar
        onDeactivate={noop}
        onExecute={(expr) => {
          executed = expr;
        }}
      />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill('resource.observation.kind == "Pod"');
    const menu = component.locator(".ome-search__autocomplete");
    await expect(menu).toBeVisible();
    await page.keyboard.press("Enter");
    expect(executed).toBe('resource.observation.kind == "Pod"');
    await expect(menu).toBeVisible();
  });

  test("clicking result closes menu", async ({ mount }) => {
    const resultClicked = { value: false };
    const component = await mount(
      <AdvancedSearchBar
        onDeactivate={noop}
        onExecute={noop}
        results={
          <button onClick={() => (resultClicked.value = true)}>
            Result Item
          </button>
        }
        lastFilter="test"
      />,
    );
    const menu = component.locator(".ome-search__autocomplete");
    await expect(menu).toBeVisible();
    await menu.getByText("Result Item").click();
    await expect(menu).toBeHidden();
  });

  test("input text has color-coded tokens via mirror overlay", async ({
    mount,
  }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const input = component.getByLabel("Advanced CEL search");
    await input.fill('resource.observation.kind == "Pod"');
    const mirror = component.locator(".ome-search__input-mirror");
    await expect(mirror).toBeVisible();
    await expect(mirror.locator(".ome-search__cel-token--field")).toBeVisible();
    await expect(
      mirror.locator(".ome-search__cel-token--operator"),
    ).toBeVisible();
    await expect(mirror.locator(".ome-search__cel-token--value")).toBeVisible();
  });

  test("mirror overlay hidden when input is empty", async ({ mount }) => {
    const component = await mount(
      <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
    );
    const mirror = component.locator(".ome-search__input-mirror");
    await expect(mirror).toBeHidden();
  });

  test.describe("history integration", () => {
    const HISTORY_ENTRIES: SeedEntry[] = [
      {
        expression: 'resource.observation.kind == "Pod"',
        timestamp: Date.now() - 60_000,
        favorite: false,
      },
      {
        expression: "resource.conditions.Ready.status",
        timestamp: Date.now() - 120_000,
        favorite: true,
      },
    ];

    test.beforeEach(async ({ page }) => {
      await clearHistory(page);
      await seedHistory(page, HISTORY_ENTRIES);
    });

    test.afterEach(async ({ page }) => {
      await clearHistory(page);
    });

    test("renders favorites and recent groups", async ({ mount }) => {
      const component = await mount(
        <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
      );
      const menu = component.locator(".ome-search__autocomplete");
      await expect(menu.getByText("Favorites")).toBeVisible();
      await expect(menu.getByText("Recent")).toBeVisible();
      await expect(
        menu.getByText("resource.conditions.Ready.status"),
      ).toBeVisible();
      await expect(
        menu.getByText('resource.observation.kind == "Pod"'),
      ).toBeVisible();
    });

    test("arrow keys navigate from suggestions into history", async ({
      mount,
      page,
    }) => {
      const component = await mount(
        <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
      );
      const menu = component.locator(".ome-search__autocomplete");
      // Wait for history to load from IDB
      await expect(menu.getByText("Favorites")).toBeVisible();
      // 3 field suggestions, then favorites (1), then recent (1) = 5 total
      // Press ArrowDown 3 times to pass field suggestions
      await page.keyboard.press("ArrowDown");
      await page.keyboard.press("ArrowDown");
      await page.keyboard.press("ArrowDown");
      // pf-m-focus is on the <li> wrapper, not the inner <button role=menuitem>
      const favoriteItem = menu.locator("li.pf-m-focus");
      await expect(favoriteItem).toBeVisible();
      await expect(favoriteItem).toContainText(
        "resource.conditions.Ready.status",
      );
    });

    test("arrow keys wrap from last history to first suggestion", async ({
      mount,
      page,
    }) => {
      const component = await mount(
        <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
      );
      const menu = component.locator(".ome-search__autocomplete");
      // Wait for history to load from IDB
      await expect(menu.getByText("Recent")).toBeVisible();
      // ArrowUp from index 0 wraps to the last item (recent entry)
      await page.keyboard.press("ArrowUp");
      const focusedItem = menu.locator("li.pf-m-focus");
      await expect(focusedItem).toBeVisible();
      await expect(focusedItem).toContainText(
        'resource.observation.kind == "Pod"',
      );
    });

    test("Tab on history item loads expression without executing", async ({
      mount,
      page,
    }) => {
      let executed = "";
      const component = await mount(
        <AdvancedSearchBar
          onDeactivate={noop}
          onExecute={(expr) => {
            executed = expr;
          }}
        />,
      );
      const input = component.getByLabel("Advanced CEL search");
      const menu = component.locator(".ome-search__autocomplete");
      // Wait for history to load from IDB
      await expect(menu.getByText("Favorites")).toBeVisible();
      // Navigate to first history item (index 3 = past 3 suggestions)
      await page.keyboard.press("ArrowDown");
      await page.keyboard.press("ArrowDown");
      await page.keyboard.press("ArrowDown");
      await page.keyboard.press("Tab");
      const value = await input.inputValue();
      expect(value).toBe("resource.conditions.Ready.status");
      expect(executed).toBe("");
    });

    test("Enter on history item loads and executes expression", async ({
      mount,
      page,
    }) => {
      let executed = "";
      const component = await mount(
        <AdvancedSearchBar
          onDeactivate={noop}
          onExecute={(expr) => {
            executed = expr;
          }}
        />,
      );
      const input = component.getByLabel("Advanced CEL search");
      const menu = component.locator(".ome-search__autocomplete");
      // Wait for history to load from IDB
      await expect(menu.getByText("Favorites")).toBeVisible();
      // Navigate to first history item (index 3 = past 3 suggestions)
      await page.keyboard.press("ArrowDown");
      await page.keyboard.press("ArrowDown");
      await page.keyboard.press("ArrowDown");
      await page.keyboard.press("Enter");
      const value = await input.inputValue();
      expect(value).toBe("resource.conditions.Ready.status");
      expect(executed).toBe("resource.conditions.Ready.status");
    });

    test("clicking history item loads expression", async ({ mount }) => {
      const component = await mount(
        <AdvancedSearchBar onDeactivate={noop} onExecute={noop} />,
      );
      const input = component.getByLabel("Advanced CEL search");
      const menu = component.locator(".ome-search__autocomplete");
      // Wait for history to load from IDB
      await expect(menu.getByText("Favorites")).toBeVisible();
      await menu
        .getByRole("menuitem", { name: /resource\.conditions\.Ready\.status/ })
        .click();
      const value = await input.inputValue();
      expect(value).toBe("resource.conditions.Ready.status");
    });
  });
});
