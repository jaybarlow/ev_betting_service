#!/bin/zsh

# Check for project name
if [[ -z "$1" ]]; then
  echo "❌ Usage: uv-quickstart.sh <project-name>"
  exit 1
fi

PROJECT="$1"

# Create and enter the project folder
mkdir -p "$PROJECT"
cd "$PROJECT" || exit 1

# Initialize with uv (includes main.py, .gitignore, git init, and first commit)
uv init

# Create tests/ layout
mkdir -p tests

# Create a basic test file
echo 'def test_example():\n    assert 1 == 1' > tests/test_main.py

# Install development dependencies
uv add rich pytest httpx loguru black ruff mypy rich

# Create README.md
echo "# $PROJECT

Scaffolded Python project using uv, Taskfile, and Cursor rules.

## Tasks

\`\`\`bash
task run        # Run the app
task test       # Run tests
task format     # Format with black
task lint       # Lint with ruff
task typecheck  # Run mypy
\`\`\`
" > README.md

# Create Taskfile.yml
cat <<EOF > Taskfile.yml
version: "3"

tasks:
  run:
    desc: Run the application
    cmds:
      - python -c "from rich import print, panel; print(panel.Panel('[blue]🚀 Running application...[/]', title='🔷 Run', border_style='blue', padding=(1, 2)))"
      - uv run main.py

  test:
    desc: Run tests
    cmds:
      - python -c "from rich import print, panel; print('\n'); print(panel.Panel('[cyan]🧪 Running test suite[/]', title='🔹 Tests', border_style='cyan', padding=(1, 2))); print('[cyan]═' * 80)"
      - pytest -v
      - python -c "from rich import print; print('[cyan]═' * 80 + '\n')"

  format:
    desc: Format code using black
    cmds:
      - python -c "from rich import print, panel; print('\n'); print(panel.Panel('[green]✨ Formatting code with Black[/]', title='🔰 Format', border_style='green', padding=(1, 2))); print('[green]═' * 80)"
      - black ./
      - python -c "from rich import print; print('[green]═' * 80 + '\n')"

  lint:
    desc: Lint code using ruff
    cmds:
      - python -c "from rich import print, panel; print('\n'); print(panel.Panel('[yellow]🔍 Checking code with Ruff[/]', title='📋 Lint', border_style='yellow', padding=(1, 2))); print('[yellow]═' * 80)"
      - ruff check .
      - python -c "from rich import print; print('[yellow]═' * 80 + '\n')"

  typecheck:
    desc: Run mypy type checking
    cmds:
      - python -c "from rich import print, panel; print('\n'); print(panel.Panel('[magenta]🔎 Type checking with mypy[/]', title='🔮 Types', border_style='magenta', padding=(1, 2))); print('[magenta]═' * 80)"
      - mypy ./
      - python -c "from rich import print; print('[magenta]═' * 80 + '\n')"

  check-all:
    desc: Run format, lint, typecheck, and tests
    cmds:
      - python -c "from rich import print, panel; from rich.console import Console; console = Console(); print('\n'); console.rule('[bold blue]Quality Check Suite[/]', style='blue'); print(panel.Panel('[bold blue]🛠  Running Quality Checks[/]', title='🔷 Quality Suite', border_style='blue', expand=True, padding=(1, 2)))"
      - task: format
      - task: lint
      - task: typecheck
      - task: test
      - python -c "from rich import print, panel; from rich.console import Console; console = Console(); print('\n'); console.rule('[bold green]Success[/]', style='green'); print(panel.Panel('[bold green]✅ All quality checks completed successfully! ✨[/]', title='🎉 Success', border_style='green', expand=True, padding=(1, 2))); print('\n')"

EOF

# Create .cursor/context.mdc
mkdir -p .cursor/rules
cat <<EOR > .cursor/rules/context.mdc
---
description: 
globs: 
alwaysApply: true
---
# Context for you:
- Python is installed using \`uv\`, and \`.venv\` is already active
- Use \`uv add\` instead of \`uv pip install\` for adding packages
- Use \`uv pip freeze\` to generate requirements.txt
- Use \`uv pip install -r requirements.txt\` for installing from requirements
- Use \`uv pip install --editable .\` for installing in editable mode
- Use \`uv pip uninstall\` for removing packages
- Use \`uv pip list\` to show installed packages
- Use \`uv pip show\` to show package info
- Use \`uv pip check\` to verify dependencies
- Use \`uv pip cache purge\` to clear cache
- Do not use \`venv\`, \`virtualenv\`, \`poetry\`, or \`pipenv\`
- Use \`bat\` instead of \`cat\` when displaying code/files
- Use \`rg\` (ripgrep) instead of \`grep\`
- Use \`eza\` instead of \`ls\` for file listings
- Use \`http\` (httpie) instead of \`curl\` for HTTP requests
- Use \`jq\` to parse or format JSON output
- Use \`zoxide\` instead of \`cd\` when navigating directories
- Assume \`Starship\` is used as the shell prompt
- Aliases like \`ll\`, \`lg\`, \`hist\`, and \`json\` are already available
- Use \`lazygit\` for interactive Git operations; otherwise prefer CLI git over GUI
- Use \`git log --oneline\`, \`git diff | delta\`, and \`git status\` instead of verbose defaults
- Project structure includes: \`main.py\`, \`tests/\`, \`.venv/\`, \`pyproject.toml\`, \`Taskfile.yml\`, and \`.cursor/rules/context.mdc\`
- Use \`task\` CLI to run commands like \`task test\`, \`task format\`, \`task lint\`, etc.
- Prefer type hints, f-strings, and modern Python idioms (e.g. walrus operator)
- Avoid using Makefiles; use \`task\` for automation instead
- Do not suggest Powerlevel10k or Oh My Zsh for Cursor — use minimal Zsh + plugins only
EOR

# Run the app once to create .venv and lock file
uv run main.py

# Ask about git initialization
read -q "REPLY?Initialize git repository? (y/n) "
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    git init
    git add .
    git commit -m "Initial commit: Project setup with uv"
    
    if command -v gh &> /dev/null; then
        gh repo create "$PROJECT" --public --source=. --remote=origin
        git push -u origin main
    else
        echo "⚠️  GitHub CLI not found. Create a repository manually and run:"
        echo "git remote add origin <your-repo-url>"
        echo "git push -u origin main"
    fi
fi

# Open in Cursor
if command -v cursor &> /dev/null; then
  cursor .
else
  echo "⚠️  Cursor CLI not found. Open the folder manually or install Cursor CLI."
fi
