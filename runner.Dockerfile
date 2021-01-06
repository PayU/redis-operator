FROM docker-registry.zooz.co:4567/public-docker-registry/zooz/docker.compose:1.24.1

ARG BUILD_DATE

# Metadata
LABEL org.label-schema.name="helm-kubectl" \
   org.label-schema.build-date=$BUILD_DATE

ENV KUBE_VERSION="v1.20.0"
ENV HELM_VERSION="v3.4.2"

# Inatall helm & kubectl
RUN apk add --no-cache ca-certificates bash git openssh curl \
   && wget -q https://storage.googleapis.com/kubernetes-release/release/${KUBE_VERSION}/bin/linux/amd64/kubectl -O /usr/local/bin/kubectl \
   && chmod +x /usr/local/bin/kubectl \
   && wget -q https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz -O - | tar -xzO linux-amd64/helm > /usr/local/bin/helm \
   && chmod +x /usr/local/bin/helm
