# associations_updater
Script python para realizar la bajada de planilla de asociación de categorías a dominios.

Modo de uso:
```javascript
python associations_updater.py -e development | production
```

Al especificar el ambiente production se hace la bajada contra la base de datos MySQL del Migration Utils. El csv debe poseer el nombre: ProductIdentifiers.csv
