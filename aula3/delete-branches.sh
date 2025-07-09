#!/bin/bash

# Nome da branch que você quer manter
BRANCH_KEEP="main"

# Garante que você está na branch principal antes de deletar outras
git checkout "$BRANCH_KEEP"

# Lista todas as branches locais, exceto a que você quer manter
for branch in $(git branch | grep -v "^\*" | grep -v "$BRANCH_KEEP"); do
  echo "Deletando branch: $branch"
  git branch -D "$branch"
done
