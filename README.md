# Quick Distribute
> Quick run python script on multi-pc in multi-process.

## Installation

Quick install

```bash
python3 setup.py install
```

or install to the specified directory

```bash
python3 setup.py install --prefix=/path/to/install
```

. Verify the installation.

```bash
quickdist --help
```

## Usage

### For work node

1. Mount directories with `sshfs` or `nfs`.
2. Config mount point with:
    - `quickdist config origin /path/to/origin`
    - `quickdist config local /path/to/local`
    - `quickdist config temp /path/to/temp`
    - `quickdist config origin.video /path/to/video`
3. Start serve with `quickdist serve`.

If it is not processing files, the first two steps can be skipped.

### For work monster

1. Write work script like:

```python
import os

def init():
    pid = int(os.environ.get('PID', 0))
    print(f'initialized {__file__} in pid={pid}')

def main(*args, **kwargs):
    pid = int(os.environ.get('PID', 0))
    prefix = [f'main[{pid}](']
    params = []
    for v in args:
        params.append(repr(v))
    for k, v in kwargs.items():
        params.append(f'{str(k)}={repr(v)}')
    suffix = [')']
    msg = ''.join([*prefix, ', '.join(params), *suffix])
    return msg
```

The `main` is required, and `init` is optional function.

The `main` is the work function to run on multi-pc in multi-process.

2. Do works on nodes.

```python
from quickdist.monster import Monster

if __name__ == '__main__':
    monster = Monster()
    monster.connect('localhost')
    monster.setup('entry.py')
    print(monster.call(1, '2', 3))
```

The monster will distribute this call to the node. And get result.

```text
main[0](1, '2, 3')
```

## How file sharing

...
