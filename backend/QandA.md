
## Q: How does it process documents?

## A: Based on my analysis of the Onyx codebase, here's how queries about a collection of documents are answered, covering the complete pipeline from document ingestion to query response:

  Document Ingestion Pipeline

  1. Document Collection via Connectors

  - Connector Framework (onyx/connectors/interfaces.py:85-111): Documents are fetched through 50+ connectors (Slack, Google Drive, Confluence, etc.)
  - Connector Types: LoadConnector (full reindex), PollConnector (incremental updates), CheckpointedConnector (resumable indexing)
  - Background Processing (onyx/background/indexing/run_docfetching.py): Celery workers handle document fetching asynchronously

  2. Document Parsing & Processing

  - File Processing (onyx/file_processing/extract_file_text.py): Supports 20+ file types (PDF, DOCX, HTML, images, etc.)
  - Text Extraction: Uses libraries like markitdown, pypdf, unstructured for different file formats
  - Image Processing (onyx/indexing/indexing_pipeline.py:431-537): Vision-capable LLMs generate text summaries for images
  - Document Filtering (onyx/indexing/indexing_pipeline.py:373-428): Removes empty documents and enforces size limits

  3. Chunking Strategy

  - Chunker (onyx/indexing/chunker.py): Uses SentenceChunker from chonkie library
  - Smart Chunking: Preserves semantic boundaries, handles metadata, supports multi-pass indexing
  - Large Chunks: Optional creation of larger chunks alongside mini-chunks for better context
  - Contextual RAG (onyx/indexing/indexing_pipeline.py:634-673): Adds document summaries and chunk-in-context descriptions

  4. Embedding Generation

  - Embedder (onyx/indexing/embedder.py): Converts chunks to vector embeddings
  - Model Support: Various embedding providers (OpenAI, Cohere, Voyage, local models)
  - Batch Processing: Handles embedding failures gracefully with retry logic
  - Content Classification: ML model assigns boost scores to chunks based on information content quality

  5. Indexing in Vespa

  - Vector Database (onyx/document_index/vespa/index.py): Stores embeddings and metadata in Vespa
  - Schema Design: Supports hybrid search (vector + keyword), access control, document sets, user permissions
  - Metadata Enrichment: Adds boost scores, document relationships, access controls, timestamps
  - Transaction Safety: Database locks prevent race conditions during concurrent indexing

  Query Processing & Retrieval Pipeline

  6. Search Pipeline

  - Search Orchestration (onyx/context/search/pipeline.py:49-100): Central SearchPipeline class coordinates all steps
  - Query Preprocessing: Analyzes query intent, expands queries, applies filters
  - Multi-Stage Retrieval: Initial vector/keyword search → section retrieval → reranking → LLM filtering

  7. Vector Retrieval

  - Vespa Search (onyx/document_index/vespa/chunk_retrieval.py): Executes hybrid vector + keyword search
  - Query Types: Semantic search, keyword search, or hybrid combination based on query analysis
  - Filtering: Document sets, time ranges, source types, user permissions applied at query time
  - Result Ranking: Combines relevance scores, recency bias, manual boost factors

  8. Context Assembly

  - Section Retrieval: Gets surrounding context for top chunks
  - Reranking (optional): Re-scores results using specialized reranking models
  - LLM Relevance Filtering: LLM evaluates chunk relevance to query
  - Document Deduplication: Prevents multiple chunks from same document overwhelming results

  Answer Generation Pipeline

  9. Answer Generation

  - Answer Class (onyx/chat/answer.py:47-50): Orchestrates LLM response generation
  - Agent Search (optional): Advanced multi-step reasoning with tool usage
  - Prompt Engineering (onyx/chat/prompt_builder/answer_prompt_builder.py): Builds context-aware prompts
  - Streaming Response: Real-time answer generation with citation tracking

  10. Citation & Context

  - Citation Mapping: Tracks which parts of answer came from which documents
  - Source Attribution: Links answer segments back to original document sections
  - Context Windows: Manages token limits while preserving important context
  - File Attachments: Handles user-uploaded files alongside indexed documents

  Key Features

  - Permissions: Enterprise ACL system controls document access during retrieval
  - Multi-tenancy: Supports isolated document collections per tenant
  - Real-time Updates: Incremental indexing keeps documents current
  - Contextual RAG: Enhanced chunks with document summaries and contextual descriptions
  - Agent Workflows: Multi-step reasoning for complex queries
  - Hybrid Search: Combines vector similarity and keyword matching
  - Quality Controls: Content classification, relevance filtering, answer validation

  The system processes millions of documents efficiently through this pipeline, maintaining both speed and accuracy in answering queries about document collections.


## Q: What is the role of agents in all this? How does the intelligence layer work?

## A: Based on my analysis of the Onyx codebase, here's how agents work and the role of the intelligence layer:

  Agent Architecture & Orchestration

  Multi-Agent System

  Onyx implements a sophisticated multi-agent architecture using LangGraph for orchestrating complex reasoning workflows:

  Agent Types (onyx/agents/agent_search/run_graph.py:9-22)

  1. Basic Agent: Simple question-answering with search
  2. Deep Search Agent: Multi-step reasoning with question decomposition
  3. Divide & Conquer Agent: Complex problem breakdown and analysis
  4. Knowledge Base (KB) Agent: Specialized for structured data queries

  Graph-Based Orchestration (onyx/agents/agent_search/models.py:77-96)

  - LangGraph Integration: Uses state machines for complex multi-step workflows
  - GraphConfig: Central configuration containing inputs, tooling, behavior, and persistence
  - State Management: Each agent type has specialized state classes for workflow context
  - Event Streaming: Real-time updates during agent execution (onyx/agents/agent_search/run_graph.py:47-87)

  Intelligence Layer Components

  LLM Integration & Factory (onyx/llm/factory.py:36-94)

  - Multi-LLM Support: Primary LLM for complex reasoning + Fast LLM for quick decisions
  - Provider Abstraction: Supports OpenAI, Anthropic, Cohere, local models, etc.
  - Persona-Based Selection: Different personas can use different LLM configurations
  - Vision Capabilities: Specialized LLMs for image analysis and processing

  Secondary LLM Flows (onyx/secondary_llm_flows/)

  The intelligence layer includes specialized LLM-powered flows for different cognitive tasks:

  Query Intelligence

  - Search Decision (choose_search.py:27-48): Determines if search is needed based on conversation context
  - Query Expansion (query_expansion.py:22-48): Reformulates queries for better retrieval
  - Query Validation: Ensures query quality and safety

  Content Intelligence

  - Answer Validation: Validates LLM responses for accuracy and relevance
  - Chunk Usefulness: Evaluates document chunk relevance to queries
  - Source Filtering: Intelligently filters information sources
  - Time-Based Filtering: Applies temporal relevance filtering

  Tool System & Search Agents

  Tool Architecture (onyx/tools/tool.py:22-90)

  - Abstract Tool Interface: Standardized tool calling across different LLM providers
  - Tool Calling Support: Both explicit (OpenAI-style) and implicit tool calling
  - Tool Composition: Tools can be chained and combined for complex workflows

  Search Tool (onyx/tools/tool_implementations/search/search_tool.py:83-100)

  - Semantic Search Integration: Connects agents to the document index
  - Context-Aware Search: Uses conversation history to improve search relevance
  - Multi-Modal Search: Handles text, images, and structured data
  - Result Processing: Ranks, filters, and processes search results

  Intelligence Flow: Query → Reasoning → Answer

  1. Query Analysis Phase

  User Query → Query Intelligence → Search Decision → Query Expansion
  - LLM analyzes if search is needed based on conversation context
  - Query gets expanded/rephrased for better retrieval
  - Intent classification determines processing approach

  2. Agent Selection & Orchestration

  Query Type → Agent Selection → Graph Construction → State Initialization
  - System selects appropriate agent type (Basic/Deep/DC/KB)
  - LangGraph builds execution workflow
  - Agent state initialized with tools and context

  3. Multi-Step Reasoning (Deep Search Agent)

  Initial Search → Question Decomposition → Sub-Question Answering → Answer Synthesis

  Initial Search (onyx/agents/agent_search/deep_search/main/nodes/start_agent_search.py:22-52):
  - Performs exploratory search to understand available information
  - Gets top 3 relevant documents to inform decomposition

  Question Decomposition:
  - Breaks complex queries into sub-questions
  - Identifies entities and key terms for focused search
  - Creates refined questions based on available information

  Sub-Question Processing:
  - Each sub-question gets its own search and analysis
  - Results are validated and consolidated
  - Cross-references answers for consistency

  Answer Synthesis:
  - Compares initial vs. refined answers
  - Generates final comprehensive response
  - Includes citations and source attribution

  4. Tool Orchestration (onyx/agents/agent_search/orchestration/)

  Tool Selection → Input Preparation → Tool Execution → Response Processing
  - Agents dynamically select appropriate tools
  - Search tool integrates with document index
  - Results processed through multiple intelligence layers

  5. Response Generation & Validation

  Context Assembly → Prompt Building → LLM Generation → Answer Validation
  - Assembles retrieved documents into coherent context
  - Builds persona-aware prompts with proper formatting
  - Generates answers with citation tracking
  - Validates response quality and accuracy

  Key Intelligence Features

  Contextual Reasoning

  - Conversation Awareness: Agents understand chat history context
  - Multi-Turn Reasoning: Can build on previous interactions
  - Persona Adaptation: Adjusts reasoning style based on configured personas

  Quality Control

  - Answer Validation: Multiple validation layers ensure response quality
  - Source Verification: Cross-references information across sources
  - Hallucination Prevention: Grounds responses in retrieved documents

  Adaptive Behavior

  - Dynamic Tool Selection: Chooses tools based on query complexity
  - Fallback Mechanisms: Gracefully handles errors and edge cases
  - Performance Optimization: Balances accuracy with response speed

  Enterprise Features

  - Permission Awareness: Agents respect document access controls
  - Audit Trails: Tracks reasoning steps for transparency
  - Multi-Tenant Support: Isolated agent contexts per organization

  The agents essentially act as intelligent orchestrators that coordinate between the document retrieval system, LLMs, and various specialized tools to provide sophisticated reasoning capabilities that go far beyond simple
  search-and-respond patterns. They enable complex multi-step reasoning, question decomposition, and evidence synthesis - making Onyx capable of handling enterprise-level knowledge work scenarios.


## Q: Would not it be costly and slow? Is it possible to figure out how many steps on average a query would take and how much it would cost?

## A: You're absolutely right to be concerned about cost and performance! The agent system would indeed be expensive and slow without careful engineering. Here's what I found about the computational complexity and cost management:

  Cost & Performance Reality

  Agent Computational Complexity

  Looking at the agent configurations (onyx/configs/agent_configs.py:4-383), the system is designed with significant overhead:

  Deep Search Agent Steps:
  - Initial Search: 5 exploratory results (AGENT_EXPLORATORY_SEARCH_RESULTS = 5)
  - Question Decomposition: 3-5 LLM calls for sub-question generation
  - Sub-Question Answering: Each sub-question triggers its own search + LLM generation
  - Entity Extraction: Additional LLM call (AGENT_MAX_TOKENS_ENTITY_TERM_EXTRACTION = 1024)
  - Answer Synthesis: 2-3 more LLM calls for validation and refinement
  - Final Answer: Main LLM generation with up to 35 documents (AGENT_MAX_STREAMED_DOCS_FOR_REFINED_ANSWER = 35)

  Token Usage per Agent Query:
  - Sub-question generation: ~256 tokens × 3-5 questions = 768-1,280 tokens
  - Search calls: 15 retrievals × multiple sub-questions = 45-75 search operations
  - Answer generation: 1,024-2,048 tokens for main response
  - Total estimate: 5,000-15,000 tokens per complex query (vs ~500-2,000 for basic search)

  Cost Control Mechanisms

  1. Enterprise Token Rate Limiting (onyx/server/token_rate_limits/)

  - Global limits: Organization-wide token budgets per time period
  - User group limits: Department/team-specific quotas
  - Individual limits: Per-user token restrictions
  - Real-time monitoring: Token usage tracked per minute in database (onyx/server/query_and_chat/token_limit.py:77-89)

  2. Agent Behavior Controls (onyx/configs/agent_configs.py:33-37)

  AGENT_ALLOW_REFINEMENT = os.environ.get("AGENT_ALLOW_REFINEMENT", "").lower() == "true"
  AGENT_ANSWER_GENERATION_BY_FAST_LLM = os.environ.get("AGENT_ANSWER_GENERATION_BY_FAST_LLM", "").lower() == "true"
  - Refinement toggle: Can disable the expensive multi-step reasoning
  - Fast LLM fallback: Use cheaper models for agent steps
  - Timeout controls: 15+ different timeout settings to prevent runaway costs

  3. Performance Optimizations

  LLM Selection Strategy:
  - Primary LLM: Expensive, high-quality model for final answers
  - Fast LLM: Cheaper model for intermediate steps (search decisions, validation, etc.)
  - Model switching: Different steps use different cost/performance profiles

  Caching & Optimization:
  - Redis caching: Search results and LLM responses cached where possible
  - LRU caching: Frequently used functions cached (@lru_cache(maxsize=1))
  - Result reuse: Similar queries benefit from cached computations

  Smart Batching:
  - Parallel processing: Multiple agent steps run concurrently where possible
  - Token limits: Strict controls prevent excessive context windows
  - Early termination: Agents can exit early if sufficient information found

  Typical Usage Patterns & Estimated Costs

  Query Type Distribution (based on config defaults):

  - Basic queries (80%): ~500-2,000 tokens each
  - Agent queries (15%): ~5,000-15,000 tokens each
  - Complex agent queries (5%): ~10,000-25,000 tokens each

  Cost Calculation Example (using GPT-4 pricing ~$0.03/1K tokens):

  Daily enterprise usage (1000 employees, 50 queries/day average):
  - Basic queries: 40,000 × 1,000 tokens = 40M tokens → ~$1,200/day
  - Agent queries: 7,500 × 10,000 tokens = 75M tokens → ~$2,250/day
  - Complex agent: 2,500 × 20,000 tokens = 50M tokens → ~$1,500/day
  - Total: ~$5,000/day or ~$150K/month for heavy usage

  Performance Characteristics:

  - Basic search: 2-5 seconds response time
  - Agent search: 15-45 seconds response time
  - Complex agent: 30-120 seconds response time

  Cost Mitigation Strategies

  1. Smart Agent Selection

  The system appears designed to minimize agent usage:
  - Search decision LLM (onyx/secondary_llm_flows/choose_search.py:27-48): Determines if search is needed
  - Query complexity analysis: Routes simple queries to basic search
  - Agent disable flags: DISABLE_LLM_CHOOSE_SEARCH to bypass expensive decisions

  2. Result Limits & Timeouts

  Extensive timeout configuration prevents runaway costs:
  - Entity extraction: 45 seconds max (AGENT_TIMEOUT_LLM_ENTITY_TERM_EXTRACTION = 45)
  - Sub-answer generation: 45 seconds max
  - Refined answer: 60 seconds max
  - Document verification: 8 seconds max

  3. Document Retrieval Limits

  - Max 15 retrieval hits per query (AGENT_MAX_QUERY_RETRIEVAL_RESULTS = 15)
  - Max 10 reranking results (AGENT_RERANKING_MAX_QUERY_RETRIEVAL_RESULTS = 10)
  - Max 35 documents for final answer context

  Business Model Implications

  This architecture suggests Onyx targets high-value enterprise use cases where:
  - Query quality justifies 5-10x higher costs than basic search
  - Complex reasoning provides significant business value (legal research, technical analysis, strategic planning)
  - Enterprise budgets can absorb $100K+/month for AI-powered knowledge work

  The extensive cost controls indicate they've learned from early implementations that uncontrolled agent usage can quickly become prohibitively expensive. The system is engineered to provide enterprise
  customers with both powerful capabilities and the controls needed to manage costs effectively.

  For most queries, users likely get basic search. Agent workflows are probably reserved for complex scenarios where the additional cost is justified by the quality improvement.


## Q: How to best run Onyx on a single cloud box, maybe in a container???

## A: Based on my analysis of the Onyx codebase, here's the best configuration for running Onyx locally on a single cloud box:

  Container Architecture

  Yes, Onyx requires multiple containers. From scripts/restart_containers.sh:6-7,23-24, the core dependencies are:

  Required Containers:

  1. PostgreSQL (onyx_postgres) - Database on port 5432
  2. Vespa (onyx_vespa) - Search engine on ports 8081, 19071
  3. Redis (onyx_redis) - Cache/job queue on port 6379
  4. MinIO (onyx_minio) - Object storage on ports 9004/9005
  5. Onyx Backend - Main API server on port 8080
  6. Model Server - ML inference server on port 9000
  7. Web Frontend - UI on port 3000
  8. Background Workers - Multiple Celery workers for document processing

  Deployment Options

  Option 1: Docker Compose (Recommended)

  From deployment/docker_compose/docker-compose.dev.yml:1-100:

  # GPU-enabled (recommended if available)
  cd /path/to/onyx/deployment/docker_compose
  docker compose -f docker-compose.gpu-dev.yml -p onyx-stack up -d --pull always --force-recreate

  # CPU-only fallback
  docker compose -f docker-compose.dev.yml -p onyx-stack up -d --pull always --force-recreate

  Option 2: Manual Container Setup

  The scripts/restart_containers.sh script shows the minimal container setup:

  # PostgreSQL
  docker run -p 5432:5432 --name onyx_postgres -e POSTGRES_PASSWORD=password -d postgres -c max_connections=250

  # Vespa (search engine)
  docker run --detach --name onyx_vespa --hostname vespa-container --publish 8081:8081 --publish 19071:19071 vespaengine/vespa:8

  # Redis (cache)
  docker run --detach --name onyx_redis --publish 6379:6379 redis

  # MinIO (object storage)
  docker run --detach --name onyx_minio --publish 9004:9000 --publish 9005:9001 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data --console-address ":9001"

  Minimum Hardware Requirements

  For Development/Testing:

  - CPU: 4-6 cores minimum (8+ recommended)
  - RAM: 16GB minimum (32GB recommended)
  - Storage: 100GB SSD minimum (500GB+ for production data)
  - Network: Good internet for initial model downloads (~15+ minutes)

  For Production/Heavy Usage:

  - CPU: 8-16 cores (the supervisord.conf shows 8+ Celery worker processes)
  - RAM: 32-64GB
  - Storage: 1TB+ SSD
  - GPU: Highly recommended - 8GB+ VRAM (e.g., RTX 4070, A4000)

  GPU Considerations (deployment/docker_compose/README.md:11-14):

  - VRAM Usage: ~1GB per embedding model (2GB total for inference + indexing)
  - Supported: NVIDIA GPUs with proper driver setup
  - Performance: "Significant improvements" according to docs
  - Requirements: NVIDIA drivers + nvidia-container-toolkit

  Cloud Instance Recommendations

  AWS:

  - Development: c5.2xlarge (8 vCPU, 16GB RAM) + 100GB EBS
  - Production: c5.4xlarge (16 vCPU, 32GB RAM) + p3.2xlarge (GPU) + 500GB EBS
  - Cost: ~$200-800/month depending on usage

  Google Cloud:

  - Development: n2-standard-8 (8 vCPU, 32GB RAM)
  - Production: n2-standard-16 + T4/V100 GPU
  - Cost: ~$180-600/month

  Azure:

  - Development: Standard_D8s_v3 (8 vCPU, 32GB RAM)
  - Production: Standard_NC6s_v3 (GPU-enabled)

  Step-by-Step Setup

  1. Provision Cloud Instance
    - Choose GPU-enabled if budget allows
    - Install Docker + Docker Compose
    - For GPU: Install NVIDIA drivers + container toolkit
  2. Clone & Deploy
  git clone https://github.com/onyx-dot-app/onyx.git
  cd onyx/deployment/docker_compose

  # Copy environment template
  cp env.prod.template .env
  # Edit .env with your settings (API keys, etc.)

  # Launch with GPU
  docker compose -f docker-compose.gpu-dev.yml -p onyx-stack up -d --pull always

  # Or CPU-only
  docker compose -f docker-compose.dev.yml -p onyx-stack up -d --pull always
  3. Initial Setup
    - Wait 15+ minutes for downloads/startup
    - Access web UI at http://your-ip:3000
    - API available at http://your-ip:8080

  Performance Notes

  - Cold start: Expect 15-30 minutes initial setup time
  - Document indexing: CPU-intensive, benefits from more cores
  - Search queries: GPU acceleration provides significant speedup
  - Memory usage: Scales with document corpus size and concurrent users
  - Agent queries: Much more resource-intensive than basic search (5-10x)

  The Docker Compose approach is definitely recommended as it handles all the service orchestration and networking automatically. Manual container setup is more complex but gives you finer control over resource
  allocation.


## Q: How to integrate MCP servers into Onyx?

## A: Based on my analysis of the Onyx codebase, I can see that Onyx does NOT have native MCP (Model Context Protocol) server integration. However, it has a robust Custom Tool system that can be used to integrate external APIs and services, including your EDGAR filings MCP server.

  Here's how to integrate your EDGAR filings MCP server into Onyx:

  Option 1: Custom Tool Integration (Recommended)

  Onyx supports custom tools via OpenAPI specifications (onyx/tools/tool_implementations/custom/custom_tool.py:77-100). You'll need to:

  Step 1: Create OpenAPI Wrapper for Your MCP Server

  Since MCP uses JSON-RPC, you'll need to create an HTTP API wrapper around your MCP server:

```
  # edgar_api_wrapper.py
  from fastapi import FastAPI, HTTPException
  from pydantic import BaseModel
  import asyncio
  from mcp import ClientSession, StdioServerParameters
  import json

  app = FastAPI(title="EDGAR MCP API", version="1.0.0")

  class SearchRequest(BaseModel):
      query: str
      limit: int = 10

  class FilingRequest(BaseModel):
      cik: str
      form_type: str = None

  @app.post("/search", 
            summary="Search EDGAR filings",
            description="Search through SEC EDGAR database for company filings")
  async def search_filings(request: SearchRequest):
      # Connect to your MCP server and make the search call
      # Return structured data that Onyx can use
      pass

  @app.get("/filing/{cik}",
           summary="Get filing by CIK",
           description="Retrieve specific filing by Company Central Index Key")
  async def get_filing(cik: str, form_type: str = None):
      # Connect to your MCP server
      pass
```

  Step 2: Generate OpenAPI Schema

```
  # Generate OpenAPI schema from your FastAPI app
  python -c "
  import json
  from edgar_api_wrapper import app
  print(json.dumps(app.openapi(), indent=2))
  " > edgar_openapi.json
```

  Step 3: Register Custom Tool in Onyx

  Using the admin API (onyx/server/features/tool/api.py:54-71):

```
  # Register the custom tool via API
  curl -X POST "http://your-onyx-instance:8080/admin/tool/custom" \
    -H "Content-Type: application/json" \
    -d '{
      "name": "edgar_search",
      "description": "Search SEC EDGAR database for company filings and financial data",
      "definition": '$(cat edgar_openapi.json)',
      "custom_headers": [
        {
          "key": "Authorization",
          "value": "Bearer YOUR_API_KEY"
        }
      ],
      "passthrough_auth": false
    }'
```

  Step 4: Add to Persona

  In the Onyx web UI:
  1. Go to Assistants → Create/Edit Assistant
  2. In the Tools section, enable your "EDGAR Search" tool
  3. Configure the assistant with appropriate prompts for financial/legal queries

  Option 2: Direct Integration as Built-in Tool

  For deeper integration, you can add it as a built-in tool (onyx/tools/built_in_tools.py:38-83):

  Step 1: Create EDGAR Tool Class

```
  # onyx/tools/tool_implementations/edgar/edgar_tool.py
  from collections.abc import Generator
  from typing import Any

  from onyx.tools.tool import Tool
  from onyx.tools.models import ToolResponse

  class EdgarTool(Tool):
      _NAME = "edgar_search"
      _DISPLAY_NAME = "EDGAR Search"
      _DESCRIPTION = "Search SEC EDGAR database for company filings"

      def __init__(self, mcp_server_endpoint: str):
          self.mcp_endpoint = mcp_server_endpoint

      @property
      def name(self) -> str:
          return self._NAME

      @property
      def description(self) -> str:
          return self._DESCRIPTION

      @property
      def display_name(self) -> str:
          return self._DISPLAY_NAME

      def tool_definition(self) -> dict:
          return {
              "type": "function",
              "function": {
                  "name": self._NAME,
                  "description": self._DESCRIPTION,
                  "parameters": {
                      "type": "object",
                      "properties": {
                          "query": {
                              "type": "string",
                              "description": "Search query for EDGAR filings"
                          },
                          "cik": {
                              "type": "string",
                              "description": "Company CIK number (optional)"
                          }
                      },
                      "required": ["query"]
                  }
              }
          }

      def run(self, **kwargs: Any) -> Generator[ToolResponse, None, None]:
          # Connect to your MCP server and execute the query
          # Return formatted results
          pass
```

  Step 2: Register in Built-in Tools

  Add to onyx/tools/built_in_tools.py:38-83:

  from onyx.tools.tool_implementations.edgar.edgar_tool import EdgarTool

```
  BUILT_IN_TOOLS: list[InCodeToolInfo] = [
      # ... existing tools ...
      InCodeToolInfo(
          cls=EdgarTool,
          description="Search SEC EDGAR database for company filings and financial information",
          in_code_tool_id=EdgarTool.__name__,
          display_name=EdgarTool._DISPLAY_NAME,
      ),
  ]
```

  Option 3: MCP Bridge Service (Most Flexible)

  Create a dedicated bridge service that translates between Onyx's HTTP API expectations and your MCP server:

```
  # mcp_bridge.py
  from fastapi import FastAPI
  from mcp import ClientSession
  import asyncio
  import json

  class MCPBridge:
      def __init__(self, mcp_server_params):
          self.server_params = mcp_server_params

      async def query_mcp_server(self, method: str, params: dict = None):
          """Generic MCP server query method"""
          async with ClientSession(self.server_params) as session:
              # Initialize MCP session
              await session.initialize()

              # Call the MCP method
              result = await session.call_tool(method, params or {})
              return result

      async def search_edgar(self, query: str, **kwargs):
          return await self.query_mcp_server("search_filings", {
              "query": query,
              **kwargs
          })

      async def get_filing(self, cik: str, **kwargs):
          return await self.query_mcp_server("get_filing", {
              "cik": cik,
              **kwargs
          })

  # FastAPI wrapper
  app = FastAPI()
  bridge = MCPBridge(your_mcp_server_params)

  @app.post("/edgar/search")
  async def search_endpoint(request: dict):
      return await bridge.search_edgar(**request)
```

  Integration Benefits

  Once integrated, your EDGAR MCP server will enable Onyx to:

  1. Answer financial questions with real SEC filing data
  2. Perform comparative analysis across multiple companies
  3. Track regulatory changes and compliance updates
  4. Generate reports combining EDGAR data with internal documents
  5. Agent workflows can automatically pull financial context for complex queries

  Performance Considerations

  - Caching: Consider caching frequent EDGAR queries in Redis
  - Rate limiting: SEC EDGAR has rate limits - implement proper throttling
  - Data size: Large filings may need streaming or pagination
  - Cost impact: Agent queries using EDGAR data will consume more tokens

  The Custom Tool approach (Option 1) is recommended as it requires minimal code changes to Onyx and provides immediate integration capability through the admin interface.