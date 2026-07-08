#!/bin/bash
# Register custom Search Attributes in Temporal for this PoC.
# Run this once after starting the Temporal server.

echo "📋 Registering custom Search Attributes..."

temporal operator search-attribute create \
  --name userId \
  --type Keyword \
  2>/dev/null && echo "  ✅ userId (Keyword)" || echo "  ℹ️  userId already exists"

temporal operator search-attribute create \
  --name subscriptionType \
  --type Keyword \
  2>/dev/null && echo "  ✅ subscriptionType (Keyword)" || echo "  ℹ️  subscriptionType already exists"

echo ""
echo "Done! You can now search workflows by:"
echo '  temporal workflow list -q '\''userId="user-001"'\'''
echo '  temporal workflow list -q '\''subscriptionType="BILL"'\'''
echo '  temporal workflow list -q '\''userId="user-001" AND subscriptionType="P2P"'\'''
