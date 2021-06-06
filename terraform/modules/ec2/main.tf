resource "aws_instance" "instance" {
  ami           = data.aws_ami.latest-ubuntu.id
  instance_type = var.instance_type

  tags = local.tags.instance

  iam_instance_profile   = aws_iam_instance_profile.role_profile.name

  key_name               = var.keypair_name
  vpc_security_group_ids = [aws_security_group.sg_instance.id]
  user_data              = data.template_file.user_data.rendered
  subnet_id              = var.subnet_id

  root_block_device {
    volume_size = var.volume_size
    volume_type = var.volume_type
  }
}

data "aws_ami" "latest-ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}
