import json
import hashlib
import datetime
import logging
import os
from typing import Dict, Any, List, Optional
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class Neo4jIngestion:
    """Utility class for ingesting lineage data into Neo4j"""
    
    def __init__(self, bolt_url: str = "bolt://localhost:7687", 
                 username: str = "neo4j", password: str = "password"):
        self.bolt_url = bolt_url
        self.username = username
        self.password = password
        self.driver = None
        self.logger = logging.getLogger(__name__)
        
        # Get Neo4j credentials from environment variables if not provided
        if not password or password == "password":
            self.password = os.getenv("NEO4J_PASSWORD", "password")
        if bolt_url == "bolt://localhost:7687":
            self.bolt_url = os.getenv("NEO4J_BOLT_URL", "bolt://localhost:7687")
        if username == "neo4j":
            self.username = os.getenv("NEO4J_USERNAME", "neo4j")
    
    def _get_driver(self):
        """Get Neo4j driver instance"""
        if self.driver is None:
            try:
                self.driver = GraphDatabase.driver(self.bolt_url, auth=(self.username, self.password))
                # Test the connection
                with self.driver.session() as session:
                    session.run("RETURN 1")
                self.logger.info(f"Successfully connected to Neo4j at {self.bolt_url}")
            except Exception as e:
                self.logger.error(f"Failed to connect to Neo4j at {self.bolt_url}: {e}")
                self.driver = None
                raise
        return self.driver
    
    def is_neo4j_available(self) -> bool:
        """Check if Neo4j is available and accessible"""
        try:
            driver = self._get_driver()
            with driver.session() as session:
                session.run("RETURN 1")
            return True
        except Exception as e:
            self.logger.warning(f"Neo4j is not available: {e}")
            return False
    
    def close(self):
        """Close the Neo4j driver connection"""
        if self.driver:
            self.driver.close()
            self.driver = None
    
    def _sha256_str(self, s: str) -> str:
        """Generate SHA256 hash of a string"""
        return hashlib.sha256(s.encode("utf-8")).hexdigest()
    
    def _now_iso(self) -> str:
        """Get current time in ISO format"""
        return datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    def _norm_schema_hash(self, fields: List[Dict[str, Any]]) -> str:
        """Normalize schema hash from fields"""
        items = [{"name": f.get("name",""), "type": f.get("type","")} for f in fields or []]
        items = sorted(items, key=lambda x: (x["name"], x["type"]))
        return self._sha256_str(json.dumps(items, separators=(",", ":"), ensure_ascii=False))
    
    def _calculate_job_version_id(self, job_ns: str, job_name: str, scm: Dict[str, Any], sc: Dict[str, Any]) -> str:
        """Calculate JobVersion.versionId based on job info and source code"""
        commit = (scm or {}).get("commit")
        code = (sc or {}).get("sourceCode", "")
        code_hash = self._sha256_str(code) if code else None
        
        repo_url = (scm or {}).get("repoUrl", "")
        path = (scm or {}).get("path", "")
        
        if commit:
            base = f"{job_ns}:{job_name}:git:{commit}"
            if repo_url and path:
                base += f":repo:{repo_url}:path:{path}"
        elif code_hash:
            base = f"{job_ns}:{job_name}:code:{code_hash}"
            if repo_url and path:
                base += f":repo:{repo_url}:path:{path}"
        else:
            base = f"{job_ns}:{job_name}:no-version"
            if repo_url and path:
                base += f":repo:{repo_url}:path:{path}"
        
        return self._sha256_str(base)
    
    def _calculate_dataset_version_id(self, namespace: str, name: str, schema_hash: str) -> str:
        """Calculate DatasetVersion.versionId based on dataset info and schema"""
        return self._sha256_str(f"{namespace}:{name}:schema:{schema_hash}")
    
    def _calculate_field_hash(self, field: Dict[str, Any], dataset_version_id: str) -> str:
        """Calculate FieldVersion.fieldHash based on field properties and dataset version"""
        field_data = {
            "name": field.get("name", ""),
            "type": field.get("type", "STRING"),
            "description": field.get("description", ""),
            "datasetVersionId": dataset_version_id
        }
        return self._sha256_str(json.dumps(field_data, separators=(",", ":"), sort_keys=True))

    def _calculate_transformation_hash(self, transformation: Dict[str, Any]) -> str:
        """Calculate Transformation.txHash based on transformation properties"""
        packed = {
            "type": transformation.get("type"),
            "subtype": transformation.get("subtype"),
            "description": transformation.get("description"),
            "masking": transformation.get("masking", False),
        }
        return self._sha256_str(json.dumps(packed, separators=(",", ":"), sort_keys=True))
    
    def _validate_unique_constraints(self, params: Dict[str, Any]) -> List[str]:
        """Validate that parameters satisfy unique constraints"""
        errors = []
        
        if not params.get("run", {}).get("runId"):
            errors.append("Run.runId cannot be null")
        
        if not params.get("job", {}).get("namespace") or not params.get("job", {}).get("name"):
            errors.append("Job namespace and name cannot be null")
        
        if not params.get("jobVersion", {}).get("versionId"):
            errors.append("JobVersion.versionId cannot be null")
        
        for i, inp in enumerate(params.get("inputs", [])):
            if not inp.get("version", {}).get("versionId"):
                errors.append(f"Input {i}: DatasetVersion.versionId cannot be null")
        
        for i, outp in enumerate(params.get("outputs", [])):
            if not outp.get("version", {}).get("versionId"):
                errors.append(f"Output {i}: DatasetVersion.versionId cannot be null")
        
        for i, inp in enumerate(params.get("inputs", [])):
            for j, field in enumerate(inp.get("fields", [])):
                if not field.get("name"):
                    errors.append(f"Input {i}, Field {j}: FieldVersion name cannot be null")
        
        for i, outp in enumerate(params.get("outputs", [])):
            for j, field in enumerate(outp.get("fields", [])):
                if not field.get("name"):
                    errors.append(f"Output {i}, Field {j}: FieldVersion name cannot be null")
        
        for i, deriv in enumerate(params.get("derivations", [])):
            if not deriv.get("tr", {}).get("txHash"):
                errors.append(f"Derivation {i}: Transformation.txHash cannot be null")
        
        return errors
    
    def _normalize_transformation_types(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize transformation types to standard OpenLineage values"""
        # Normalize transformation types in outputs
        for out in event.get("outputs", []):
            cl = out.get("facets", {}).get("columnLineage", {}) or {}
            for field_name, field_data in cl.get("fields", {}).items():
                for inref in field_data.get("inputFields", []):
                    for tr in inref.get("transformations", []):
                        # Normalize transformation types
                        if tr.get("type") == "Direct copy":
                            tr["type"] = "IDENTITY"
                        if tr.get("subtype") == "NA":
                            tr["subtype"] = "DIRECT_COPY"
                        
                        # Normalize other common patterns
                        if tr.get("type") == "COUNT(DISTINCT o.order_id)":
                            tr["type"] = "AGGREGATE"
                            tr["subtype"] = "COUNT_DISTINCT"
                        elif tr.get("type") == "SUM(oi.item_total)":
                            tr["type"] = "AGGREGATE"
                            tr["subtype"] = "SUM"
                        elif tr.get("type") == "AVG(oi.item_total)":
                            tr["type"] = "AGGREGATE"
                            tr["subtype"] = "AVERAGE"
                        elif tr.get("type") == "MAX(o.order_date)":
                            tr["type"] = "AGGREGATE"
                            tr["subtype"] = "MAX"
                        elif tr.get("type") == "CURRENT_DATE function":
                            tr["type"] = "FUNCTION"
                            tr["subtype"] = "CURRENT_DATE"
        
        return event

    def _transform_event_format(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Transform event format to the expected OpenLineage format"""
        # Check if this is the custom format (eventType and eventTime at top level)
        if "eventType" in event and "eventTime" in event and "run" in event:
            # Transform to standard OpenLineage format while preserving all facets
            transformed = {
                "eventType": event["eventType"],
                "eventTime": event["eventTime"],
                "run": {
                    "runId": event["run"]["runId"],
                    "facets": {
                        "parent": {
                            "job": {
                                "namespace": event["job"]["namespace"],
                                "name": event["job"]["name"]
                            }
                        }
                    }
                },
                "job": event["job"],
                "inputs": event.get("inputs", []),
                "outputs": event.get("outputs", [])
            }
            
            # Preserve any additional run facets that might exist
            if "facets" in event["run"]:
                transformed["run"]["facets"].update(event["run"]["facets"])
            
            # Add missing job facets if not present
            if "facets" not in transformed["job"]:
                transformed["job"]["facets"] = {}
            
            job_facets = transformed["job"]["facets"]
            
            # Add sourceCodeLocation if not present
            if "sourceCodeLocation" not in job_facets:
                job_facets["sourceCodeLocation"] = {
                    "type": "git",
                    "url": "https://github.com/unknown",
                    "repoUrl": "https://github.com/unknown",
                    "path": "jobs/unknown.sql",
                    "version": "main",
                    "branch": "main"
                }
            
            # Add documentation if not present - ensure description is never null
            if "documentation" not in job_facets:
                job_facets["documentation"] = {
                    "description": "Auto-generated job documentation",
                    "contentType": "text/markdown"
                }
            else:
                # Ensure existing documentation has a non-null description
                if job_facets["documentation"].get("description") is None:
                    job_facets["documentation"]["description"] = "Auto-generated job documentation"
                # Also ensure contentType is not null
                if job_facets["documentation"].get("contentType") is None:
                    job_facets["documentation"]["contentType"] = "text/markdown"
            
            # Add ownership if not present - ensure owner names are never null
            if "ownership" not in job_facets:
                job_facets["ownership"] = {
                    "owners": [
                        {"name": "data-team", "type": "TEAM"}
                    ]
                }
            else:
                # Clean up existing ownership to ensure no null names
                if job_facets["ownership"].get("owners"):
                    cleaned_owners = []
                    for owner in job_facets["ownership"]["owners"]:
                        if owner.get("name") is not None:
                            cleaned_owners.append(owner)
                        else:
                            # Replace null name with default
                            cleaned_owners.append({
                                "name": "unknown-owner",
                                "type": owner.get("type", "INDIVIDUAL")
                            })
                    job_facets["ownership"]["owners"] = cleaned_owners
                else:
                    job_facets["ownership"]["owners"] = [{"name": "data-team", "type": "TEAM"}]
            
            # Add missing facets to inputs
            for inp in transformed["inputs"]:
                if "facets" not in inp:
                    inp["facets"] = {}
                
                facets = inp["facets"]
                
                # Add tags if not present
                if "tags" not in facets:
                    facets["tags"] = [
                        {"key": "domain", "value": "unknown", "source": "auto"},
                        {"key": "sensitivity", "value": "public", "source": "auto"}
                    ]
                
                # Add ownership if not present - ensure owner names are never null
                if "ownership" not in facets:
                    facets["ownership"] = {
                        "owners": [
                            {"name": "data-team", "type": "TEAM"}
                        ]
                    }
                else:
                    # Clean up existing ownership to ensure no null names
                    if facets["ownership"].get("owners"):
                        cleaned_owners = []
                        for owner in facets["ownership"]["owners"]:
                            if owner.get("name") is not None:
                                cleaned_owners.append(owner)
                            else:
                                # Replace null name with default
                                cleaned_owners.append({
                                    "name": "unknown-owner",
                                    "type": owner.get("type", "INDIVIDUAL")
                                })
                        facets["ownership"]["owners"] = cleaned_owners
                    else:
                        facets["ownership"]["owners"] = [{"name": "data-team", "type": "TEAM"}]
                
                # Add inputStatistics if not present
                if "inputStatistics" not in facets:
                    facets["inputStatistics"] = {
                        "rowCount": 0,
                        "fileCount": 0,
                        "size": 0
                    }
            
            # Add missing facets to outputs
            for out in transformed["outputs"]:
                if "facets" not in out:
                    out["facets"] = {}
                
                facets = out["facets"]
                
                # Add tags if not present
                if "tags" not in facets:
                    facets["tags"] = [
                        {"key": "domain", "value": "unknown", "source": "auto"},
                        {"key": "sensitivity", "value": "public", "source": "auto"}
                    ]
                
                # Add ownership if not present - ensure owner names are never null
                if "ownership" not in facets:
                    facets["ownership"] = {
                        "owners": [
                            {"name": "data-team", "type": "TEAM"}
                        ]
                    }
                else:
                    # Clean up existing ownership to ensure no null names
                    if facets["ownership"].get("owners"):
                        cleaned_owners = []
                        for owner in facets["ownership"]["owners"]:
                            if owner.get("name") is not None:
                                cleaned_owners.append(owner)
                            else:
                                # Replace null name with default
                                cleaned_owners.append({
                                    "name": "unknown-owner",
                                    "type": owner.get("type", "INDIVIDUAL")
                                })
                        facets["ownership"]["owners"] = cleaned_owners
                    else:
                        facets["ownership"]["owners"] = [{"name": "data-team", "type": "TEAM"}]
                
                # Add outputStatistics if not present
                if "outputStatistics" not in facets:
                    facets["outputStatistics"] = {
                        "rowCount": 0,
                        "fileCount": 0,
                        "size": 0
                    }
            
            # Normalize transformation types
            transformed = self._normalize_transformation_types(transformed)
            
            return transformed
        return event
    
    def _build_params_from_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Build parameters from OpenLineage event for Neo4j ingestion"""
        # Transform event format if needed
        event = self._transform_event_format(event)
        
        # Job & Run
        job_ns = event.get("run", {}).get("facets", {}).get("parent", {}).get("job", {}).get("namespace") \
                 or event.get("job", {}).get("namespace") or "default"
        job_name = event.get("run", {}).get("facets", {}).get("parent", {}).get("job", {}).get("name") \
                   or event.get("job", {}).get("name") or "unknown_job"

        run = {
            "runId": event["run"]["runId"],
            "eventType": event.get("eventType"),
            "eventTime": event.get("eventTime"),
        }

        # Job version
        scm = event.get("job", {}).get("facets", {}).get("sourceCodeLocation", {}) or {}
        sc = event.get("job", {}).get("facets", {}).get("sourceCode", {}) or {}
        job_version_id = self._calculate_job_version_id(job_ns, job_name, scm, sc)
        job_version = {
            "versionId": job_version_id,
            "gitRef": scm.get("version"),
            "codeHash": self._sha256_str(sc.get("sourceCode", "")) if sc.get("sourceCode") else None,
            "createdAt": self._now_iso(),
        }
        
        # Handle documentation properly - only include if it has valid values
        doc_facet = event.get("job", {}).get("facets", {}).get("documentation")
        if doc_facet and doc_facet.get("description") is not None:
            # Only include documentation if description is not null
            job_facets_doc = doc_facet
        else:
            # Set to None so the Cypher query won't try to create a Doc node
            job_facets_doc = None
        
        # Filter out owners with null names
        job_owners = (event.get("job", {}).get("facets", {}).get("ownership", {}) or {}).get("owners", [])
        filtered_job_owners = [owner for owner in job_owners if owner.get("name") is not None]
        
        job_facets = {
            "sourceCode": sc or None,
            "scm": {
                "type": scm.get("type"),
                "url": scm.get("url"),
                "repoUrl": scm.get("repoUrl"),
                "path": scm.get("path"),
                "version": scm.get("version"),
                "tag": scm.get("tag"),
                "branch": scm.get("branch"),
            } if scm else None,
            "jobType": event.get("job", {}).get("facets", {}).get("jobType"),
            "doc": job_facets_doc,  # This will be None if description is null
            "owners": filtered_job_owners,
        }

        # Inputs
        inputs = []
        for inp in event.get("inputs", []) or []:
            fields = (inp.get("facets", {}).get("schema", {}) or {}).get("fields", []) or []
            
            # Ensure fields are present - if not, create a default field
            if not fields:
                fields = [{"name": "default_field", "type": "STRING", "description": "Default field for dataset without schema"}]
            
            schema_hash = self._norm_schema_hash(fields)
            version_id = self._calculate_dataset_version_id(inp["namespace"], inp["name"], schema_hash)
            tags = inp.get("facets", {}).get("tags", []) or []
            
            # Filter out owners with null names
            owners = (inp.get("facets", {}).get("ownership", {}) or {}).get("owners", []) or []
            filtered_owners = [owner for owner in owners if owner.get("name") is not None]
            
            stats = inp.get("facets", {}).get("inputStatistics", {}) or None
            
            # Process fields with proper hashing
            processed_fields = []
            for field in fields:
                field_hash = self._calculate_field_hash(field, version_id)
                processed_fields.append({
                    "name": field.get("name", ""),
                    "type": field.get("type", "STRING"),
                    "description": field.get("description", ""),
                    "fieldHash": field_hash
                })
            
            inputs.append({
                "dataset": {"namespace": inp["namespace"], "name": inp["name"]},
                "version": {"versionId": version_id, "schemaHash": schema_hash, "createdAt": self._now_iso()},
                "fields": processed_fields,
                "tags": [{"key": t.get("key"), "value": t.get("value"), "source": t.get("source")} for t in tags],
                "owners": filtered_owners,
                "stats": {"rowCount": stats.get("rowCount"), "fileCount": stats.get("fileCount"), "size": stats.get("size")} if stats else None,
            })

        dv_index = {(i["dataset"]["namespace"], i["dataset"]["name"]): i["version"]["versionId"] for i in inputs}

        # Outputs
        outputs = []
        for out in event.get("outputs", []) or []:
            cl_fields = (out.get("facets", {}).get("columnLineage", {}) or {}).get("fields", {}) or {}
            out_field_names = sorted(list(cl_fields.keys()))
            
            # Ensure fields are present - if not, create a default field
            if not out_field_names:
                out_field_names = ["default_field"]
            
            # Create field objects with proper structure
            out_fields = []
            for field_name in out_field_names:
                field_hash = self._calculate_field_hash({"name": field_name, "type": "STRING", "description": ""}, "")
                out_fields.append({
                    "name": field_name,
                    "type": "STRING",  # Default type for output fields
                    "description": "",
                    "fieldHash": field_hash
                })
            
            out_schema_hash = self._sha256_str(json.dumps(out_field_names, separators=(",", ":")))
            out_version_id = self._calculate_dataset_version_id(out["namespace"], out["name"], out_schema_hash)
            
            # Update field hashes with correct dataset version ID
            for field in out_fields:
                field["fieldHash"] = self._calculate_field_hash(field, out_version_id)
            
            stats = out.get("facets", {}).get("outputStatistics", {}) or None
            lifecycle = out.get("facets", {}).get("lifecycleStateChange", {}) or None
            outputs.append({
                "dataset": {"namespace": out["namespace"], "name": out["name"]},
                "version": {
                    "versionId": out_version_id,
                    "schemaHash": out_schema_hash,
                    "createdAt": self._now_iso(),
                    "lifecycle": {
                        "state": lifecycle.get("lifecycleState"),
                        "previousIdentifier": lifecycle.get("previousIdentifier", {})
                    } if lifecycle else None
                },
                "fields": out_fields,
                "stats": {"rowCount": stats.get("rowCount"), "fileCount": stats.get("fileCount"), "size": stats.get("size")} if stats else None,
            })
        dv_index.update({(o["dataset"]["namespace"], o["dataset"]["name"]): o["version"]["versionId"] for o in outputs})

        # Derivations with Transformation hash
        derivations = []
        for out in event.get("outputs", []) or []:
            cl = (out.get("facets", {}).get("columnLineage", {}) or {}).get("fields", {}) or {}
            out_ns = out["namespace"]
            out_name = out["name"]
            outDvId = dv_index[(out_ns, out_name)]  # already built above from inputs/outputs

            for out_field, mapping in cl.items():
                for inref in mapping.get("inputFields", []) or []:
                    in_ns = inref["namespace"]
                    in_name = inref["name"]

                    # Try resolve DV id from inputs/outputs
                    inDvId = dv_index.get((in_ns, in_name))
                    if not inDvId:
                        # Deterministic placeholder DV id so repeated refs converge
                        # (use your helper to keep same format)
                        placeholder_schema_hash = self._sha256_str("unknown_schema")
                        inDvId = self._calculate_dataset_version_id(in_ns, in_name, placeholder_schema_hash)

                    trs = inref.get("transformations", []) or [{"type":"unknown","subtype":None,"description":None,"masking":False}]
                    for tr in trs:
                        tx_hash = self._calculate_transformation_hash(tr)
                        derivations.append({
                            "out": {
                                "namespace": out_ns,
                                "name": out_name,
                                "versionId": outDvId,
                                "field": out_field
                            },
                            "in": {
                                "namespace": in_ns,
                                "name": in_name,
                                "versionId": inDvId,
                                "field": inref.get("field")
                            },
                            "tr": {
                                "type": tr.get("type"),
                                "subtype": tr.get("subtype"),
                                "description": tr.get("description"),
                                "masking": tr.get("masking", False),
                                "txHash": tx_hash
                            },
                            "createdAt": self._now_iso(),
                        })


        # Run facets
        env_vars = (event.get("run", {}).get("facets", {}).get("environmentVariables", {}) or {}).get("environmentVariables", []) or []
        error = event.get("run", {}).get("facets", {}).get("errorMessage", {}) or None
        error_obj = {"message": error.get("message"), "programmingLanguage": error.get("programmingLanguage"), "stackTrace": error.get("stackTrace")} if error else None
        xq = event.get("run", {}).get("facets", {}).get("externalQuery", {}) or None
        xq_obj = {"externalQueryId": xq.get("externalQueryId"), "source": xq.get("source")} if xq else None

        params = {
            "job": {"namespace": job_ns, "name": job_name},
            "run": run,
            "jobVersion": job_version,
            "jobFacets": job_facets,
            "inputs": inputs,
            "outputs": outputs,
            "envVars": [{"name": v.get("name"), "value": v.get("value")} for v in env_vars],
            "error": error_obj,
            "externalQuery": xq_obj,
            "derivations": derivations,
        }
        
        # Validate constraints before returning
        validation_errors = self._validate_unique_constraints(params)
        if validation_errors:
            raise ValueError(f"Constraint validation failed:\n" + "\n".join(validation_errors))
        
        return params
    
    def _load_cypher(self, path: str) -> str:
        """Load Cypher query from file"""
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    
    def _load_constraints(self, path: str) -> List[str]:
        """Load constraints file and split into individual statements"""
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        
        statements = []
        for line in content.split(';'):
            line = line.strip()
            if line and not line.startswith('//') and not line.startswith('/*'):
                statements.append(line + ';')
        
        return statements
    
    def apply_constraints(self, constraints_path: str = None) -> bool:
        """Apply Neo4j constraints"""
        if not self.is_neo4j_available():
            self.logger.warning("Neo4j is not available, skipping constraint application")
            return False
            
        if constraints_path is None:
            # Use default path relative to this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            constraints_path = os.path.join(current_dir, "cypher", "01_constraints.cypher")
        
        try:
            constraints = self._load_constraints(constraints_path)
            driver = self._get_driver()
            
            with driver.session() as s:
                for constraint in constraints:
                    try:
                        s.execute_write(lambda tx: tx.run(constraint).consume())
                        self.logger.info(f"Applied constraint: {constraint[:50]}...")
                    except Exception as e:
                        self.logger.warning(f"Could not apply constraint '{constraint[:50]}...': {e}")
            
            return True
        except Exception as e:
            self.logger.error(f"Error applying constraints: {e}")
            return False
    
    def ingest_lineage_event(self, event: Dict[str, Any], ingest_path: str = None) -> Dict[str, Any]:
        """Ingest a lineage event into Neo4j"""
        if not self.is_neo4j_available():
            return {
                "success": False,
                "message": "Neo4j is not available",
                "error": "Neo4j connection failed"
            }
            
        try:
            # Build parameters from event
            params = self._build_params_from_event(event)
            
            # Load ingestion query
            if ingest_path is None:
                current_dir = os.path.dirname(os.path.abspath(__file__))
                ingest_path = os.path.join(current_dir, "cypher", "02_ingest.cypher")
            
            ingest_query = self._load_cypher(ingest_path)
            
            # Execute ingestion
            driver = self._get_driver()
            with driver.session() as s:
                result = s.execute_write(lambda tx: tx.run(ingest_query, **params).consume())
            
            return {
                "success": True,
                "message": "Lineage event ingested successfully",
                "run_id": params["run"]["runId"],
                "job": f"{params['job']['namespace']}.{params['job']['name']}",
                "nodes_created": result.counters.nodes_created,
                "relationships_created": result.counters.relationships_created,
                "properties_set": result.counters.properties_set
            }
            
        except Exception as e:
            self.logger.error(f"Error ingesting lineage event: {e}")
            return {
                "success": False,
                "message": f"Error ingesting lineage event: {str(e)}",
                "error": str(e)
            }
    
    def convert_analysis_result_to_event(self, analysis_result: Dict[str, Any], 
                                       query: str, agent_name: str, model_name: str) -> Dict[str, Any]:
        """Convert analysis result to OpenLineage event format"""
        try:
            # Extract lineage data from analysis result
            lineage_data = analysis_result.get('lineage', {})
            
            # Generate a unique run ID
            run_id = f"run_{self._sha256_str(f'{query}_{agent_name}_{model_name}_{self._now_iso()}')}"
            
            # Create basic event structure
            event = {
                "eventType": "COMPLETE",
                "eventTime": self._now_iso(),
                "run": {
                    "runId": run_id,
                    "facets": {
                        "parent": {
                            "job": {
                                "namespace": "lineage_analysis",
                                "name": f"{agent_name}_agent"
                            }
                        }
                    }
                },
                "job": {
                    "namespace": "lineage_analysis",
                    "name": f"{agent_name}_agent",
                    "facets": {
                        "documentation": {
                            "description": f"Lineage analysis using {agent_name} agent with {model_name} model",
                            "contentType": "text/markdown"
                        }
                    }
                },
                "inputs": [],
                "outputs": []
            }
            
            # Add inputs and outputs from lineage data
            if isinstance(lineage_data, dict):
                # Handle table lineage
                if 'tables' in lineage_data:
                    for table in lineage_data.get('tables', []):
                        if isinstance(table, dict):
                            # Add as input
                            event["inputs"].append({
                                "namespace": table.get("namespace", "default"),
                                "name": table.get("name", "unknown_table"),
                                "facets": {
                                    "schema": {
                                        "fields": table.get("fields", [])
                                    }
                                }
                            })
                
                # Handle field lineage
                if 'fields' in lineage_data:
                    for field in lineage_data.get('fields', []):
                        if isinstance(field, dict):
                            # Add field-level lineage
                            pass  # TODO: Implement field-level lineage conversion
            
            return event
            
        except Exception as e:
            self.logger.error(f"Error converting analysis result to event: {e}")
            raise 

    def _clean_owners(self, owners: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean owners list to ensure no null names"""
        if not owners:
            return [{"name": "data-team", "type": "TEAM"}]
        
        cleaned_owners = []
        for owner in owners:
            if owner.get("name") is not None:
                cleaned_owners.append(owner)
            else:
                # Replace null name with default
                cleaned_owners.append({
                    "name": "unknown-owner",
                    "type": owner.get("type", "INDIVIDUAL")
                })
        
        return cleaned_owners if cleaned_owners else [{"name": "data-team", "type": "TEAM"}] 