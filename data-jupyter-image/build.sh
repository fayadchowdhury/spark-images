VERSION="1.0.2"
REGISTRY="fayadchowdhury"
IMAGE_NAME="data-jupyter-image"

echo "Building ${REGISTRY}/${IMAGE_NAME}:${VERSION} and latest, and pushing to registry..."

echo "Checking for Docker Buildx builder instance..."
if ! docker buildx ls | grep -q "multi-builder"; then
  echo "Creating Docker Buildx builder instance 'multi-builder'..."
  docker buildx create --name multi-builder --driver docker-container --bootstrap --use
else
  echo "Docker Buildx builder instance 'multi-builder' already exists. Using it..."
  docker buildx use multi-builder
fi

docker buildx build \
  --platform linux/amd64 \
  -t ${REGISTRY}/${IMAGE_NAME}:${VERSION} \
  -t ${REGISTRY}/${IMAGE_NAME}:latest \
  --build-arg DELTA_VERSION=4.0.0 \
  --build-arg SCALA_VERSION=2.13 \
  --build-arg HADOOP_VERSION=3.4.1 \
  --push \
  .

echo "✅ Done! Image: ${REGISTRY}/${IMAGE_NAME}:${VERSION} and latest"