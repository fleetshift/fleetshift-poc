import type { NavLayoutGroup } from "@fleetshift/common";
import { CUSTOM_GROUP_PREFIX, slugify } from "@fleetshift/common";
import {
  Button,
  Form,
  FormGroup,
  FormHelperText,
  HelperText,
  HelperTextItem,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  TextInput,
} from "@patternfly/react-core";
import { useCallback, useEffect, useMemo, useState } from "react";

import GroupIconPicker from "./GroupIconPicker";

export interface GroupFormData {
  name: string;
  description: string;
  keywords: string[];
  icon: string;
}

interface GroupFormModalProps {
  /** null -> create mode, NavLayoutGroup -> edit mode */
  editGroup: NavLayoutGroup | null;
  isOpen: boolean;
  existingGroupIds: Set<string>;
  onSave: (data: GroupFormData) => void;
  onClose: () => void;
}

function GroupFormModal({
  editGroup,
  isOpen,
  existingGroupIds,
  onSave,
  onClose,
}: GroupFormModalProps) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [keywords, setKeywords] = useState("");
  const [icon, setIcon] = useState<string | null>(null);

  useEffect(() => {
    if (!isOpen) return;
    if (editGroup) {
      setName(editGroup.label);
      setDescription(editGroup.description ?? "");
      setKeywords((editGroup.keywords ?? []).join(", "));
      setIcon(editGroup.icon ?? null);
    } else {
      setName("");
      setDescription("");
      setKeywords("");
      setIcon(null);
    }
  }, [isOpen, editGroup]);

  const generatedId = useMemo(
    () => `${CUSTOM_GROUP_PREFIX}${slugify(name)}`,
    [name],
  );

  const nameError = useMemo(() => {
    if (!name.trim()) return "Name is required";
    if (!slugify(name))
      return "Name must contain at least one alphanumeric character";
    if (!editGroup && existingGroupIds.has(generatedId)) {
      return "A group with this name already exists";
    }
    if (
      editGroup &&
      generatedId !== editGroup.groupId &&
      existingGroupIds.has(generatedId)
    ) {
      return "A group with this name already exists";
    }
    return null;
  }, [name, generatedId, existingGroupIds, editGroup]);

  const descriptionError = useMemo(
    () => (!description.trim() ? "Description is required" : null),
    [description],
  );

  const isValid = !nameError && !descriptionError;

  const handleSave = useCallback(() => {
    if (!isValid) return;
    const parsedKeywords = keywords
      .split(",")
      .map((k) => k.trim())
      .filter(Boolean);
    onSave({
      name: name.trim(),
      description: description.trim(),
      keywords: parsedKeywords,
      icon: icon ?? "",
    });
  }, [isValid, name, description, keywords, icon, onSave]);

  const title = editGroup ? "Edit group" : "Add group";

  return (
    <Modal variant="small" isOpen={isOpen} onClose={onClose}>
      <ModalHeader title={title} />
      <ModalBody>
        <Form>
          <FormGroup label="Name" isRequired fieldId="group-name">
            <TextInput
              id="group-name"
              value={name}
              onChange={(_e, val) => setName(val)}
              validated={name && nameError ? "error" : "default"}
              isRequired
            />
            {name && nameError ? (
              <FormHelperText>
                <HelperText>
                  <HelperTextItem variant="error">{nameError}</HelperTextItem>
                </HelperText>
              </FormHelperText>
            ) : (
              name.trim() && (
                <FormHelperText>
                  <HelperText>
                    <HelperTextItem variant="indeterminate">
                      ID: {generatedId}
                    </HelperTextItem>
                  </HelperText>
                </FormHelperText>
              )
            )}
          </FormGroup>

          <FormGroup label="Description" isRequired fieldId="group-description">
            <TextInput
              id="group-description"
              value={description}
              onChange={(_e, val) => setDescription(val)}
              validated={description && descriptionError ? "error" : "default"}
              isRequired
            />
            {description && descriptionError && (
              <FormHelperText>
                <HelperText>
                  <HelperTextItem variant="error">
                    {descriptionError}
                  </HelperTextItem>
                </HelperText>
              </FormHelperText>
            )}
          </FormGroup>

          <FormGroup label="Keywords" fieldId="group-keywords">
            <TextInput
              id="group-keywords"
              value={keywords}
              onChange={(_e, val) => setKeywords(val)}
              placeholder="Comma-separated keywords"
            />
            <FormHelperText>
              <HelperText>
                <HelperTextItem variant="indeterminate">
                  Optional. Used for search discovery.
                </HelperTextItem>
              </HelperText>
            </FormHelperText>
          </FormGroup>

          <FormGroup label="Icon" fieldId="group-icon">
            <GroupIconPicker selected={icon} onSelect={setIcon} />
          </FormGroup>
        </Form>
      </ModalBody>
      <ModalFooter>
        <Button variant="primary" onClick={handleSave} isDisabled={!isValid}>
          {editGroup ? "Save" : "Add group"}
        </Button>
        <Button variant="link" onClick={onClose}>
          Cancel
        </Button>
      </ModalFooter>
    </Modal>
  );
}

export default GroupFormModal;
