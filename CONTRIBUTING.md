# Contributing to Fluxus

Thank you for your interest in contributing to Fluxus! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct. Please read it before contributing.

## Getting Started

1. **Fork the repository** on GitHub.
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/synoptiq/go-fluxus.git
   cd go-fluxus
   ```
3. **Add the upstream repository** as a remote:
   ```bash
   git remote add upstream https://github.com/synoptiq/go-fluxus.git
   ```
4. **Set up your development environment**:
   ```bash
   go mod download
   ```

## Development Workflow

1. **Create a branch** for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** and write tests for them.

3. **Run tests locally** to ensure they pass:
   ```bash
   make test
   ```

4. **Run linters** to ensure code quality:
   ```bash
   make lint
   ```

5. **Commit your changes** with a clear, descriptive message:
   ```bash
   git commit -m "feat: add new feature X"
   ```

   We follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages.

6. **Push your branch** to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create a Pull Request** from your fork to the upstream repository.

## Pull Request Guidelines

When submitting a pull request:

1. **Include tests** for any new functionality.
2. **Update documentation** if needed.
3. **Describe the changes** in detail in the PR description.
4. **Link any related issues** using keywords like "Fixes #123" or "Closes #456".
5. **Ensure all CI checks pass** before requesting a review.

## Code Style

We follow standard Go code style conventions:

1. Run `go fmt` on your code before committing.
2. Use meaningful variable and function names.
3. Write godoc-style comments for exported functions, types, and constants.
4. Follow the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments) guidelines.

## Testing

- Every new feature or bug fix should include tests.
- Run the full test suite with race detection before submitting your PR:
  ```bash
  make test-race
  ```
- For performance-critical changes, include benchmarks:
  ```bash
  make benchmark
  ```

## Documentation

- Update the README.md if your changes affect the public API or user experience.
- Add godoc-style comments to any new exported functions, types, or constants.
- Consider adding examples for new features in the `examples/` directory.

## Reporting Bugs

When reporting bugs:

1. **Use the issue tracker** on GitHub.
2. **Describe the bug** in detail.
3. **Provide a minimal reproduction** case, if possible.
4. **Include your environment details** (Go version, OS, etc.).

## Feature Requests

We welcome feature requests! When submitting a feature request:

1. **Check existing issues** to see if it's already been requested.
2. **Describe the feature** in detail.
3. **Explain the use case** for the feature.
4. **Consider contributing** the feature yourself if possible.

## License

By contributing to Fluxus, you agree that your contributions will be licensed under the project's [GPL-3.0 License](LICENSE).

## Questions

If you have any questions about contributing, please open an issue or reach out to the maintainers.

Thank you for your contributions!