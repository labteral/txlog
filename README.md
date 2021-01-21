# txlog
Crash-resistant Python code made easy

## Usage
```python
from txlog import TxLog, Call
self.txlog = TxLog('./txlog')
```

### Script
```python
def secure_function(a, b):
    print(f'{a}, {b}')
    
txlog.begin()
txlog.add(Call('secure_function', ['input1', 'input2']))
txlog.commit()

txlog.exec_uncommitted_calls()
```

### Instance
```python
class Instance:
  
  def __init__(self):
    txlog.begin()
    txlog.add(Call('secure_method', ['input1', 'input2']))
    txlog.commit()
    instance.txlog.exec_uncommitted_calls(self)

  def secure_method(self, a, b):
      print(f'{a}, {b}')

instance = Instance()
```
