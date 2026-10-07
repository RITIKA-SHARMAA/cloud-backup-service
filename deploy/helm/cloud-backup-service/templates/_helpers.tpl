{{- define "cbs.name" -}}{{ .Release.Name }}-cbs{{- end -}}

{{- define "cbs.labels" -}}
app.kubernetes.io/name: cloud-backup-service
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cbs.secretName" -}}
{{- if .Values.secrets.existingSecret }}{{ .Values.secrets.existingSecret }}{{ else }}{{ include "cbs.name" . }}{{ end -}}
{{- end -}}

{{/* Environment shared by every component: settings from the ConfigMap, secrets from the Secret. */}}
{{- define "cbs.env" -}}
envFrom:
  - configMapRef:
      name: {{ include "cbs.name" . }}
  - secretRef:
      name: {{ include "cbs.secretName" . }}
{{- end -}}
