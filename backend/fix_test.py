import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('tests/executors/test_transform.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('mock_artifact mime_type', 'mock_artifact.mime_type')

with open('tests/executors/test_transform.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Fixed test_transform.py')
