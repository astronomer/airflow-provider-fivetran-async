pytest \
   -vv \
  -m "not (integration)" \
  --ignore=tests/integrations
