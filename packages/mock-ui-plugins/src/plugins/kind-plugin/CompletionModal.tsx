import {
  Button,
  Content,
  Modal,
  ModalBody,
  ModalHeader,
  Split,
  SplitItem,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import SearchIcon from "@patternfly/react-icons/dist/dynamic/icons/search-icon";
import PlusCircleIcon from "@patternfly/react-icons/dist/dynamic/icons/plus-circle-icon";
import RocketIcon from "@patternfly/react-icons/dist/dynamic/icons/rocket-icon";
import AngleRightIcon from "@patternfly/react-icons/dist/dynamic/icons/angle-right-icon";

import successImage from "./assets/day-one-complete.png";
import "./CompletionModal.scss";

interface CompletionModalProps {
  isOpen: boolean;
  onClose: () => void;
}

const actions: Array<{
  label: string;
  icon: React.ReactNode;
}> = [
  { label: "Import existing clusters", icon: <SearchIcon /> },
  { label: "Create another cluster", icon: <PlusCircleIcon /> },
  { label: "Explore more capabilities", icon: <RocketIcon /> },
];

export default function CompletionModal({
  isOpen,
  onClose,
}: CompletionModalProps) {
  return (
    <Modal variant="medium" isOpen={isOpen} onClose={onClose}>
      <ModalHeader title="" />
      <ModalBody>
        <Split hasGutter>
          <SplitItem>
            <img
              src={successImage}
              alt="Success illustration"
              className="ome-kind-completion-image"
            />
          </SplitItem>
          <SplitItem isFilled>
            <Stack hasGutter>
              <StackItem>
                <Content component="h2">
                  You successfully created <strong>your first cluster</strong>
                </Content>
              </StackItem>
              <StackItem>
                <Content component="p">
                  Now, you can create more clusters or explore additional
                  functionality.
                </Content>
                <Content component="p">
                  Choose one of the options below to continue building your
                  infrastructure or discover what your clusters can do.
                </Content>
              </StackItem>
              <StackItem>
                <Stack hasGutter>
                  {actions.map((a) => (
                    <StackItem key={a.label}>
                      <Button
                        variant="secondary"
                        isBlock
                        icon={a.icon}
                        onClick={onClose}
                      >
                        <Split>
                          <SplitItem isFilled>{a.label}</SplitItem>
                          <SplitItem>
                            <AngleRightIcon />
                          </SplitItem>
                        </Split>
                      </Button>
                    </StackItem>
                  ))}
                </Stack>
              </StackItem>
            </Stack>
          </SplitItem>
        </Split>
      </ModalBody>
    </Modal>
  );
}
