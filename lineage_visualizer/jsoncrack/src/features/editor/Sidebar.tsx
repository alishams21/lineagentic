import React, { useState } from "react";
import styled from "styled-components";
import { TextInput, Text, Paper, Stack, Title, Button } from "@mantine/core";
import useFile from "../../store/useFile";

const StyledSidebar = styled.div`
  width: 320px;
  height: 100%;
  background: ${({ theme }) => theme.BACKGROUND_SECONDARY};
  border-left: 1px solid ${({ theme }) => theme.BACKGROUND_MODIFIER_ACCENT};
  padding: 16px;
  overflow-y: auto;
`;

const StyledCard = styled(Paper)`
  background: ${({ theme }) => theme.BACKGROUND_PRIMARY};
  border: 1px solid ${({ theme }) => theme.BACKGROUND_MODIFIER_ACCENT};
  padding: 16px;
`;

const StyledInput = styled(TextInput)`
  .mantine-TextInput-input {
    background: ${({ theme }) => theme.BACKGROUND_SECONDARY};
    border: 1px solid ${({ theme }) => theme.BACKGROUND_MODIFIER_ACCENT};
    color: ${({ theme }) => theme.TEXT_NORMAL};
    
    &:focus {
      border-color: ${({ theme }) => theme.INTERACTIVE_ACTIVE};
    }
    
    &::placeholder {
      color: ${({ theme }) => theme.SILVER};
    }
  }
`;

const StyledLabel = styled.div`
  color: ${({ theme }) => theme.TEXT_NORMAL};
  font-weight: 500;
  margin-bottom: 4px;
  font-size: 14px;
`;

const StyledButton = styled(Button)`
  background: ${({ theme }) => theme.INTERACTIVE_ACTIVE};
  color: ${({ theme }) => theme.BACKGROUND_PRIMARY};
  
  &:hover {
    background: ${({ theme }) => theme.INTERACTIVE_HOVER};
  }
`;

export const Sidebar = () => {
  const [namespace, setNamespace] = useState("");
  const [datasetName, setDatasetName] = useState("");
  const [fieldName, setFieldName] = useState("");
  const [maxHops, setMaxHops] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  
  const setContents = useFile(state => state.setContents);

  const handleSubmit = async () => {
    if (!fieldName.trim()) {
      alert("Field Name is required");
      return;
    }

    setIsLoading(true);
    
    try {
      const requestBody = {
        field_name: fieldName.trim(),
        namespace: namespace.trim() || undefined,
        dataset_name: datasetName.trim() || undefined,
        max_hops: maxHops.trim() ? parseInt(maxHops.trim()) : undefined
      };

      console.log("Sending request to API:", requestBody);

      const response = await fetch("http://localhost:8000/field-lineage", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const result = await response.json();
      console.log("API Response:", result);

      if (result.success && result.data) {
        // Format the JSON data nicely and set it in the text editor
        const formattedJson = JSON.stringify(result.data, null, 2);
        setContents({ contents: formattedJson, hasChanges: false });
      } else {
        throw new Error("API returned unsuccessful response");
      }
    } catch (error) {
      console.error("Error calling API:", error);
      alert(`Error: ${error instanceof Error ? error.message : 'Unknown error occurred'}`);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <StyledSidebar>
      <StyledCard>
        <Title order={3} style={{ color: "inherit", marginBottom: "16px" }}>
          Lineagentic
        </Title>
        <Stack gap="16px">
          <div>
            <StyledLabel>Namespace</StyledLabel>
            <StyledInput
              value={namespace}
              onChange={(e) => setNamespace(e.target.value)}
              placeholder="Enter namespace"
              size="sm"
            />
          </div>

          <div>
            <StyledLabel>Dataset Name</StyledLabel>
            <StyledInput
              value={datasetName}
              onChange={(e) => setDatasetName(e.target.value)}
              placeholder="Enter dataset name"
              size="sm"
            />
          </div>

          <div>
            <StyledLabel>Field Name</StyledLabel>
            <StyledInput
              value={fieldName}
              onChange={(e) => setFieldName(e.target.value)}
              placeholder="Enter field name"
              size="sm"
            />
          </div>
          
          <div>
            <StyledLabel>Max Hops</StyledLabel>
            <StyledInput
              value={maxHops}
              onChange={(e) => setMaxHops(e.target.value)}
              placeholder="Enter max hops"
              size="sm"
            />
          </div>
          
          <StyledButton 
            onClick={handleSubmit} 
            loading={isLoading}
            disabled={isLoading || !fieldName.trim()}
            fullWidth
          >
            {isLoading ? "Loading..." : "Submit"}
          </StyledButton>
        </Stack>
      </StyledCard>
    </StyledSidebar>
  );
};
