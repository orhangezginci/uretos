# save as flatten.sh
#!/bin/zsh
find . -type f \( -name "*.js" -o -name "*.jsx" -o -name "*.ts" -o -name "*.tsx" -o -name "*.css" -o -name "*.scss" -o -name "*.json" \) \
! -path "*/node_modules/*" ! -path "*/.git/*" ! -path "*/build/*" ! -path "*/dist/*" ! -path "*/.next/*" \
| sort | while IFS= read -r file; do
  echo "### $file"
  echo '```'
  cat "$file"
  echo
  echo '```'
  echo
done > flattened-react-app.md

echo "Done → flattened-react-app.md"