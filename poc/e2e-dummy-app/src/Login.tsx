import {
  Button,
  Content,
  Form,
  FormGroup,
  Page,
  PageSection,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { useState } from "react";
import { useNavigate } from "react-router-dom";

const Login = () => {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const navigate = useNavigate();

  function handleLogin() {
    if (!username || !password) {
      alert("Please enter both username and password");
      return;
    }
    if (username === "John" && password === "password") {
      const user = {
        username,
        token: "dummy-token",
      };
      localStorage.setItem("user", JSON.stringify(user));

      navigate("/");
    } else {
      alert("Invalid username or password");
    }
  }

  return (
    <Page>
      <PageSection>
        <Title headingLevel="h1" size="lg">
          Login
        </Title>
        <div>
          <Content>Type in username John and password: password</Content>
          <Form
            onSubmit={(e) => {
              e.preventDefault();
              handleLogin();
            }}
          >
            <FormGroup name="username" label="Username" fieldId="username">
              <TextInput
                autoComplete="username"
                value={username}
                onChange={(_e, value) => setUsername(value)}
                id="username"
                name="username"
              />
            </FormGroup>

            <FormGroup name="password" label="Password" fieldId="password">
              <TextInput
                autoComplete="current-password"
                value={password}
                onChange={(_e, value) => setPassword(value)}
                id="password"
                name="password"
                type="password"
              />
            </FormGroup>
            <Button type="submit">Login</Button>
          </Form>
        </div>
      </PageSection>
    </Page>
  );
};

export default Login;
