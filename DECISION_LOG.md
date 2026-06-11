# 📓 REGISTRO DE DECISIONES TÉCNICAS (DECISION LOG)

Este documento registra el "por qué" de las decisiones arquitectónicas y cambios significativos en el Bot de Finanzas.

---

## [2026-06-11] Eliminación del Sistema de Memoria (Google Drive & NotebookLM)
- **Decisión**: Eliminar por completo el sistema de sincronización con Google Drive (`sync_docs.py`, credenciales de API) y NotebookLM.
- **Contexto**: Las API Keys de Google para el Drive de desarrollo expiraron y la integración con NotebookLM ya no aportaba suficiente valor relativo al mantenimiento técnico que requería.
- **Alternativas**: Regenerar credenciales e integrar otra API (demasiada sobrecarga).
- **Impacto**: Simplificación de las dependencias locales (`requirements-dev.txt`), eliminación de secretos obsoletos y reducción de la superficie de código a mantener.

---

## [2026-04-16] Configuración de Sistema de Memoria a Largo Plazo
- **Decisión**: Implementar un sistema de logs de decisiones y mapa de dependencias sincronizado con NotebookLM.
- **Contexto**: El proyecto ha crecido en complejidad y se requiere una fuente de verdad para que la IA (Antigravity) mantenga el contexto entre sesiones.
- **Alternativas**: Usar solo comentarios en código (insuficiente para arquitectura global).
- **Impacto**: Mejora la precisión de las sugerencias y reduce la deuda técnica.

---

## [Plantilla para nuevas entradas]
### [YYYY-MM-DD] [Título de la Decisión]
- **Decisión**: [Descripción breve]
- **Contexto**: [Problema que se intenta resolver]
- **Alternativas**: [Qué otras opciones se consideraron]
- **Impacto**: [Cómo afecta esto al sistema o al flujo de trabajo]

