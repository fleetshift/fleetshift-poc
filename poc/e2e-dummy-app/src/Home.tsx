import { Button, Page, PageSection, Title } from "@patternfly/react-core";
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

const Home = () => {
  const [user, setUser] = useState<
    { username: string; token: string } | undefined
  >(undefined);
  useEffect(() => {
    const storedUser = localStorage.getItem("user");
    if (storedUser) {
      setUser(JSON.parse(storedUser));
    } else {
      // If no user is found in localStorage, redirect to login page
      window.location.href = "/login";
    }
    // Ugly hack, don't do this, just a stupid way to force re-render if the user was removed from state/localstorage
    // this whole app will be removed anyway, so no need to fix it properly
  }, [JSON.stringify(user)]);
  return (
    <Page>
      <PageSection>
        <Title headingLevel="h1" size="lg">
          Home
        </Title>
        <div>
          <Button
            component={(props) => <Link {...props} to="/login" />}
            variant="primary"
          >
            Go to Login
          </Button>
        </div>
        <div>
          <Button
            onClick={() => {
              localStorage.removeItem("user");
              setUser(undefined);
            }}
            variant="secondary"
          >
            Clear User Data
          </Button>
        </div>
        {user && (
          <div>
            <p>Welcome, {user.username}!</p>
            <pre>{JSON.stringify(user, null, 2)}</pre>
          </div>
        )}
      </PageSection>
    </Page>
  );
};

export default Home;
