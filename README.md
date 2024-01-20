# data processor framework
```
Pipeline
    |
    |---Source (interface)
    |      |
    |      |--Read data
    |      |--Output to first Action
    |
    |---Actions
    |      |
    |      |---Action 1
    |      |       |
    |      |       |--Input from Source or previous Action's output
    |      |       |--Process
    |      |       |--Output to next Action
    |      |
    |      |---Action 2
    |              |
    |              |--Input from previous Action's output
    |              |--Process
    |              |--Output to Target
    |
    |---Target (interface)
           |
           |--Input from last Action's output
           |--Handle processed data (e.g., store, pass to another pipeline, etc.)
```
