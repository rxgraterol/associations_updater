# associations_updater
Scripts python para realizar la bajada de planilla de asociación de categorías a dominios y restringir los allowed_units en los atributos number_unit de las categorías asociadas.

## 1 - Bajada de Asociacion

Modo de uso:
```javascript
python associations_updater.py -e development | production
```

Al especificar el ambiente production se hace la bajada contra la base de datos MySQL del Migration Utils. El csv debe poseer el nombre: ProductIdentifiers.csv

## 2 - Restriccion de allowed_units

```javascript
python unit_restrictions.py -f NOMBRE_DE_PLANILLA.csv -e development | production
```
