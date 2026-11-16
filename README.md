# Delta Neutral: Autonomous High-Frequency Market-Making Engine

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Architecture: Starknet L2](https://img.shields.io/badge/L2-Starknet%20CLOB-orange.svg)](https://starknet.io/)
[![Version Control: Git](https://img.shields.io/badge/VCS-Git%20Feature--Branch-green.svg)](https://git-scm.com/)
[![License: Academic Research](https://img.shields.io/badge/license-Academic%20Research-purple.svg)]()

> **B.Tech 2nd Year Computer Science & Engineering — Version Control Systems (CS204) Capstone Project**  
> **Course Repository & Collaborative Software Architecture Demonstration**

---

## 👥 The Engineering Team & VCS Roles

This project was developed collaboratively using a structured Git **Feature-Branch Workflow**. Each member owned and implemented a dedicated architectural module:

| Name | Role in Team | Git Feature Branch | Core Responsibilities & Contributions |
| :--- | :--- | :--- | :--- |
| **Satvik (Team Lead)** | Quantitative Architect | `main`, `dev` | Core async execution engine (`AnchorEngine`), delta-neutral math ($\Delta = 0$), Starknet perpetual account signature integration. |
| **Gourang (VCS Lead)** | DevOps & Infrastructure | `feature/stream-rpc` | Git branching strategy, `.gitignore` credential protection policies, StreamRPC orderbook WebSocket streamer (`MarketDataStream`), PR reviews. |
| **Samarth** | Risk Systems Engineer | `feature/risk-governor` | `MarginGovernor` multi-slot collateral reservation, `FillIntake` 2-step sub-minimum lot dust cleanup, Pre-Trade Risk Control (PTRC) gates. |
| **Vedansh** | Telemetry & Audit Lead | `feature/ledger-telemetry` | Empirical multi-market calibration matrix (BTC, HYPE, BNB, XAG), Double-Entry on-chain clearing reconciliation ledger ($\Delta = \$0.0000$). |

---

## 🚀 Project Overview & Problem Statement

### The Problem
Traditional market makers on Central Limit Order Books (CLOBs) provide two-sided liquidity but face catastrophic **directional risk**: if the asset price plunges or surges, inventory imbalances result in severe drawdown and adverse selection.

### The Delta-Neutral Solution
Delta Neutral eliminates directional risk by operating two cryptographically separate Starknet Layer-2 accounts in a synchronized **Anchor-Hedge Loop**:
$$\Delta_{\text{portfolio}} = \Delta_{\text{Wallet A}} + \Delta_{\text{Wallet B}} = (+Q \times 1) + (-Q \times 1) \equiv 0.0000$$

1. **Wallet A (Anchor)**: Posts a single passive Post-Only Maker order at the best bid/ask touch ($0.0\text{ bps}$ maker fee).
2. **Cancellation Barrier**: Upon fill confirmation, any resting remainder is cancelled to prevent self-trade matching.
3. **Wallet B (Hedge)**: Fires an aggressive Immediate-Or-Cancel (IOC) taker hedge order within sub-150ms.
4. **Result**: Market risk is strictly decoupled ($Var(d\Pi) = 0$), capturing bid-ask spreads and liquidity rebates with zero directional market exposure.

---

## 🛠️ Git Architecture & Collaborative Workflow

Our team strictly followed professional Git version control standards:

```text
       [feature/stream-rpc] (Gourang: WebSocket Client)
              \
               --> [dev] (Integration Branch) --------> [main] (Production Release v15.2)
              /                                         /
       [feature/risk-governor] (Samarth: MarginGovernor)
              \
       [feature/ledger-telemetry] (Vedansh: Clearing Ledger)
```

### 1. Branching Strategy
- **`main`**: Protected branch containing only production-verified, mainnet-tested code. Direct commits are disallowed.
- **`dev`**: Active integration branch where features are aggregated and tested.
- **`feature/*`**: Isolated branches for individual development sprints.

### 2. Git Security & Credential Hygiene
- **Threat**: Trading bots require private keys and API credentials. Committing `.env` to public version control represents an unacceptable security vulnerability.
- **Resolution**:
  1. Configured `.gitignore` to strictly exclude `.env`, logs, and bytecode caches.
  2. Untracked existing environment files using:
     ```bash
     git rm --cached .env
     ```
  3. Committed a sanitized `.env.example` template with placeholder values so developers can configure credentials safely in their local environments.

### 3. Essential Git Commands Used Daily
```bash
# Clone the repository
git clone https://github.com/satvik77x/Delta-Neutral-Bot---extended-only---
cd Delta-Neutral-Bot

# Create and checkout a feature branch
git checkout -b feature/risk-governor

# Stage changes and make an atomic conventional commit
git add extended_v14.py
git commit -m "feat(risk): implement MarginGovernor collateral reservation"

# Rebase local commits against latest team dev changes
git pull origin dev --rebase

# Push feature branch for Pull Request review
git push -u origin feature/risk-governor

# Merge into dev with no fast-forward (preserving PR merge history)
git checkout dev
git merge --no-ff feature/risk-governor
```

---

## 📊 Live Mainnet Empirical Validation

The engine underwent live production stress-testing on the Starknet Extended L2 exchange:

| Metric | Mainnet Benchmark Run | Projected $100k Volume Scale |
| :--- | :--- | :--- |
| **Duration** | 57.7 minutes | ~72 hours |
| **Total Turnover** | **$1,156.34** | **$100,000.00** |
| **Seed Capital Base** | **$60.00** | **$60.00** |
| **Capital Velocity** | **1,666× Multiplier** | **1,666× Multiplier** |
| **Hedged Pairs Executed** | 11 pairs across 4 markets | ~1,000 pairs |
| **Blended Net Friction** | **1.866 bps ($0.22)** | **$18.66 total cost** |
| **On-Chain Clearing Drift** | **+$0.0001** | **$0.0000** |
| **Post-Session Inventory** | **100% Flat (0.00000)** | **Zero unhedged leak** |
| **Self-Trade Flags** | **0 (Zero)** | **0 (Zero)** |

---

## ⚙️ Quick Start & Local Setup

### 1. Prerequisites
- Python 3.11+
- Git 2.40+

### 2. Installation
```bash
# Clone repo
git clone https://github.com/satvik77x/Delta-Neutral-Bot---extended-only---
cd Delta-Neutral-Bot---extended-only---

# Setup virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Environment Configuration
Copy the template and insert test credentials (never commit this file):
```bash
cp .env.example .env
```

---

## 📜 Disclaimer
This software and documentation are published strictly for academic research, algorithmic risk-mitigation study, and version control software engineering demonstration. They do not constitute financial advice. All live operations strictly adhered to exchange API rate limits and Self-Trade Prevention (STP) protocols.
