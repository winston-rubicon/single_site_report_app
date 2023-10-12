# resource "aws_vpc" "main" {
#   cidr_block       = "10.0.0.0/16"
#   enable_dns_hostnames = true
#   enable_dns_support = true
# }

# resource "aws_subnet" "main" {
#   vpc_id     = aws_vpc.main.id
#   cidr_block = "10.0.1.0/24"
# }

# resource "aws_internet_gateway" "igw" {
#   vpc_id = aws_vpc.main.id
# }

# resource "aws_route_table" "public" {
#   vpc_id = aws_vpc.main.id

#   route {
#     cidr_block = "0.0.0.0/0"
#     gateway_id = aws_internet_gateway.igw.id
#   }
# }

# resource "aws_route_table_association" "a" {
#   subnet_id      = aws_subnet.main.id
#   route_table_id = aws_route_table.public.id
# }

# output "vpc_id" {
#   value = aws_vpc.main.id
# }

# output "subnet_ids" {
#   value = [aws_subnet.main.id]
# }


# resource "aws_security_group" "allow_all" {
#   name        = "allow_all"
#   description = "Allow all outbound traffic"
#   vpc_id      = aws_vpc.main.id

#   egress {
#     from_port   = 0
#     to_port     = 0
#     protocol    = "-1"
#     cidr_blocks = ["0.0.0.0/0"]
#   }

#   ingress {
#     from_port   = 443
#     to_port     = 443
#     protocol    = "tcp"
#     self        = true  # Allow all inbound traffic from the same security group
#   }
# }



# resource "aws_vpc_endpoint" "ecr_dkr" {
#   vpc_id            = aws_vpc.main.id
#   service_name      = "com.amazonaws.${var.aws_region}.ecr.dkr"
#   vpc_endpoint_type = "Interface"

#   subnet_ids = [aws_subnet.main.id]

#   security_group_ids = [aws_security_group.allow_all.id]

#   private_dns_enabled = true
# }

# resource "aws_vpc_endpoint" "ecr_api" {
#   vpc_id            = aws_vpc.main.id
#   service_name      = "com.amazonaws.${var.aws_region}.ecr.api"
#   vpc_endpoint_type = "Interface"

#   subnet_ids = [aws_subnet.main.id]

#   security_group_ids = [aws_security_group.allow_all.id]

#   private_dns_enabled = true
# }


# resource "aws_vpc_endpoint" "logs" {
#   vpc_id            = aws_vpc.main.id
#   service_name      = "com.amazonaws.${var.aws_region}.logs"
#   vpc_endpoint_type = "Interface"

#   subnet_ids = [aws_subnet.main.id]

#   security_group_ids = [aws_security_group.allow_all.id]

#   private_dns_enabled = true
# }


