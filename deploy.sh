#!/usr/bin/env bash

set -euo pipefail
cities=()
for env_file in envs/*.env; do
  [[ -e "$env_file" ]] || continue
  city_name=$(basename "$env_file" .env)
  cities+=("$city_name")
done

usage() {
  cat <<EOF
Usage: ./deploy.sh [city] [cdk-options]

Run without arguments to deploy all cities.
Specify one of: ${cities[*]}
Any additional arguments are forwarded to "cdk deploy".
EOF
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
  esac
fi

selected_cities=("${cities[@]}")
cdk_args=()

if [[ $# -gt 0 ]]; then
  first_arg="$1"
  for city in "${cities[@]}"; do
    if [[ "$first_arg" == "$city" ]]; then
      selected_cities=("$city")
      shift
      cdk_args=("$@")
      break
    fi
  done
fi

for city in "${selected_cities[@]}"; do
  stack_name="gtfs-etl-${city}"
  echo "Deploying ${stack_name}..."
  if [[ ${#cdk_args[@]} -gt 0 ]]; then
    cdk deploy -c env_file=${city} "${stack_name}" "${cdk_args[@]}"
  else
    cdk deploy -c env_file=${city} "${stack_name}"
  fi
done

