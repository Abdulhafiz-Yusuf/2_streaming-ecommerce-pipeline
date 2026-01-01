#!/bin/bash
# Automated dbt refresh script

cd ~/projects/PORTFOLIO_PROJECTS/2_streaming-ecommerce-pipeline/dbt_ecommerce_analytics

echo "🔄 Refreshing dbt models..."
dbt run --select silver+  # Run silver and downstream models

if [ $? -eq 0 ]; then
    echo "✅ dbt refresh completed successfully"
else
    echo "❌ dbt refresh failed"
    exit 1
fi