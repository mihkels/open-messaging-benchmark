{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "omb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "omb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Worker StatefulSet and headless Service name. Leaves room for the "-<ordinal>" pod suffix.
*/}}
{{- define "omb.workerName" -}}
{{- printf "%s-worker" (include "omb.fullname" . | trunc 48 | trimSuffix "-") -}}
{{- end -}}

{{- define "omb.coordinatorName" -}}
{{- printf "%s-coordinator" (include "omb.fullname" . | trunc 48 | trimSuffix "-") -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "omb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "omb.labels" -}}
helm.sh/chart: {{ include "omb.chart" . }}
{{ include "omb.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "omb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "omb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "omb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "omb.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Image reference: digest wins, otherwise <tag or appVersion>[-<variant>].
*/}}
{{- define "omb.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if .Values.image.variant -}}
{{- $tag = printf "%s-%s" $tag .Values.image.variant -}}
{{- end -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
{{- end -}}

{{/*
Comma-separated worker URLs, resolved through the headless Service in the release namespace.
*/}}
{{- define "omb.workers" -}}
{{- $worker := include "omb.workerName" . -}}
{{- $urls := list -}}
{{- range $index := until (int .Values.workers.replicas) -}}
{{- $urls = append $urls (printf "http://%s-%d.%s.%s.svc:8080" $worker $index $worker $.Release.Namespace) -}}
{{- end -}}
{{- join "," $urls -}}
{{- end -}}

{{/*
Secret holding the worker bearer token: workers.auth.existingSecret, or the one this chart creates.
*/}}
{{- define "omb.authSecretName" -}}
{{- if .Values.workers.auth.existingSecret -}}
{{- .Values.workers.auth.existingSecret -}}
{{- else -}}
{{- printf "%s-worker-token" (include "omb.fullname" . | trunc 48 | trimSuffix "-") -}}
{{- end -}}
{{- end -}}

{{- define "omb.authSecretKey" -}}
{{- default "token" .Values.workers.auth.secretKey -}}
{{- end -}}

{{/*
OMB_WORKER_TOKEN for the worker and coordinator containers when workers.auth.enabled is true.
*/}}
{{- define "omb.authEnv" -}}
{{- if .Values.workers.auth.enabled -}}
- name: OMB_WORKER_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "omb.authSecretName" . }}
      key: {{ include "omb.authSecretKey" . }}
{{- end }}
{{- end -}}

{{- define "omb.driverPath" -}}
{{- if .Values.driverConfig -}}/config/driver.yaml{{- else -}}{{ .Values.driver }}{{- end -}}
{{- end -}}

{{- define "omb.workloadPath" -}}
{{- if .Values.workloadConfig -}}/config/workload.yaml{{- else -}}{{ .Values.workload }}{{- end -}}
{{- end -}}

{{- define "omb.hasConfig" -}}
{{- if or .Values.driverConfig .Values.workloadConfig -}}true{{- end -}}
{{- end -}}

{{/*
Volumes shared by workers and the coordinator: writable /tmp and /results for the read-only root
file system, the configuration ConfigMap and user volumes.
*/}}
{{- define "omb.volumes" -}}
- name: tmp
  emptyDir: {}
- name: results
  emptyDir:
    sizeLimit: {{ .Values.resultsSizeLimit }}
{{- if include "omb.hasConfig" . }}
- name: config
  configMap:
    name: {{ include "omb.fullname" . }}-config
{{- end }}
{{- with .Values.extraVolumes }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "omb.volumeMounts" -}}
- name: tmp
  mountPath: /tmp
- name: results
  mountPath: /results
{{- if include "omb.hasConfig" . }}
- name: config
  mountPath: /config
  readOnly: true
{{- end }}
{{- with .Values.extraVolumeMounts }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Init container that waits until every worker accepts connections, so the coordinator does not
start a run against workers that are still starting.
*/}}
{{- define "omb.waitForWorkers" -}}
- name: wait-for-workers
  image: {{ include "omb.image" . }}
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  securityContext:
    {{- toYaml .Values.securityContext | nindent 4 }}
  env:
    - name: WORKERS
      value: {{ include "omb.workers" . | quote }}
  command: ["bash", "-c"]
  args:
    - |
      set -eu
      for url in ${WORKERS//,/ }; do
        hostport=${url#http://}
        host=${hostport%:*}
        port=${hostport##*:}
        until (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do
          echo "waiting for $host:$port"
          sleep 2
        done
        echo "$host:$port is up"
      done
  resources:
    requests:
      cpu: 10m
      memory: 32Mi
    limits:
      memory: 64Mi
{{- end -}}
