# Journal d'incidents

## INC-001 — JAVA_GATEWAY_EXITED

- **Date** : 16/06/2026
- **Symptôme** : SparkSession.builder.getOrCreate() plante
- **Cause** : JAVA_HOME non défini dans l'environnement VS Code
- **Solution** : os.environ["JAVA_HOME"] chargé via .env + python-dotenv
- **Statut** : Résolu

## INC-002 — CSV mal formé (reviews)

- **Date** : 17/06/2026
- **Symptôme** : review_score contient des dates et du texte brésilien
- **Cause** : Virgules dans review_comment_message décalent les colonnes
- **Solution** : Ajout des options multiLine=True, quote='"', escape='"' + schéma imposé manuellement
- **Statut** : Résolu
