data "template_file" "user_data" {
  template = file("${path.module}/user_data.sh")

  vars = {
    BUCKET_DOCKER_FILES = var.bucket_docker_files
  }
}
