pytest \
    -vv \
    --cov=dagfactory \
    --cov-report=term-missing \
    --cov-report=xml \
    -m "not (integration)" \
  --ignore=tests/integrations
