VERSION="1.0.4"
REGISTRY="fayadchowdhury"
IMAGE_NAME="bundled-spark-image"

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
  --push \
  .

echo "✅ Done! Image: ${REGISTRY}/${IMAGE_NAME}:${VERSION} and latest"