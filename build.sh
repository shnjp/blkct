set -e

# REVISION=$(git rev-parse --short HEAD)
REVISION=$(date +%Y%m%d%H%M)

for subcommand in "$@"
do
  echo $subcommand

  case $subcommand in
    format)
      black blkct
    ;;

    typing)
      mypy blkct
    ;;

    test)
      pytest
    ;;
     
    *)
      echo "[ERROR] Invalid subcommand '${1}'"
      exit 1
    ;;
  esac

done

