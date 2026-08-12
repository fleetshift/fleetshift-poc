# Web e2e tests

Skeleton app for e2e web testing.

Once e2e test env is setup, update the `playwright.config.mts`.

Update the `url` to the site URL, remove the command config (currently starts the dummy app web server).

Refer to PW docs for test writing guide: https://playwright.dev/docs/writing-tests

## Running tets

Use the `npx nx test:e2e e2e-web` command to run the tests.

For interactive session use: `npx nx test:e2e e2e-web -- --ui`.

In order to pass additional flags to the playwright command use `npx nx test:e2e e2e-web -- --<your extra flags here> --<another extra flags here> ...`