# Risk Governance & Capital Allocation Architecture

**Authored by: Samarth (Risk Systems Engineer)**

## 1. MarginGovernor Overview
- Enforces centralized collateral reservation across concurrent market pairs.
- Prevents exchange Error 1140 (Margin Collision) via atomic reservation tickets.
- Required margin calculated with a 5% price volatility haircut and 0.15% Starknet maintenance margin buffer.

## 2. FillIntake & 2-Step Roundtrip Dust Sweep
- Resolves orphan position risk when partial fills drop below minimum lot size $Q_{min}$.
- **Step 1:** Places IOC order for $Q_{min}$ in same direction (`reduce_only=False`) bringing position $\ge Q_{min}$.
- **Step 2:** Places IOC order for $(|Pos| + Q_{min})$ in opposite direction (`reduce_only=True`) clearing residual to exactly $0.00000$.
