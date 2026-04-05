# NYC Taxi Data Pipeline (Mage + PostgreSQL)

## Descripción del proyecto

Este proyecto implementa una solución **end-to-end ELT** para procesar datos históricos de NYC Taxi, utilizando:

- Mage (orquestador)
- PostgreSQL (data warehouse)
- pgAdmin (visualización y validación)
- Docker Compose (infraestructura reproducible)

El objetivo es construir una arquitectura con dos capas:

- **raw** → datos crudos e inmutables  
- **clean** → datos transformados con modelo dimensional  

---

## Arquitectura del proyecto

Flujo general:
