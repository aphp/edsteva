# Contributing

We welcome contributions ! There are many ways to help. For example, you can:

- Help us track bugs by filing issues.
- Suggest and help prioritize new functionalities.
- Develop a new [Probe][probe] or a new [Model][model] ! Fork the project and propose a new functionality through a pull request.
- Help us make the library as straightforward as possible, by simply asking questions on whatever does not seem clear to you.

## Guidelines
### 1. Development installation

Ready to contribute? Here's how to set up `edsteva` for local development.

- Fork the `edsteva` repo.
- Clone your fork locally:

    <div class="termy">

    ```console
    $ git clone git@github.com:your_name_here/edsteva.git
    ---> 100%
    ```

    </div>

- Optional, create a virtual environment:

    <div class="termy">

    ```console
    $ cd edsteva
    $ python -m venv .venv
    $ source .venv/bin/activate
    ```

    </div>

- Install [Poetry](https://python-poetry.org/) (a tool for dependency management and packaging in Python):

    === "Linux, macOS, Windows (WSL)"
        <div class="termy">
        ```console
        $ curl -sSL https://install.python-poetry.org | python3 -
        ---> 100%
        ```
        </div>

    === "Windows (Powershell)"
        <div class="termy">
        ```console
        $ (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
        ---> 100%
        ```
        </div>

    For more details, check the [installation guide](https://python-poetry.org/docs/#installation)

- Install dependencies:

    <div class="termy">

    ```console
    $ poetry config experimental.new-installer false
    $ poetry install

    color:lightblue Updating dependencies
    color:lightblue Resolving dependencies... (25.3s)

    color:lightblue Writing lock file

    Package operations: 126 installs, 0 updates, 0 removals

    • Installing ....
    • Installing ....
    ```

    </div>

- Create a branch for local development:

    <div class="termy">

    ```console
    $ git checkout -b name-of-your-bugfix-or-feature
    ```

    </div>

### 2. Style guide

We use [Black](https://github.com/psf/black) to reformat the code. While other formatter only enforce PEP8 compliance, Black also makes the code uniform.

!!! Tip
    Black reformats entire files in place. It is not configurable.

Moreover, the CI/CD pipeline enforces a number of checks on the "quality" of the code. To wit, non black-formatted code will make the test pipeline fail. To make sure the pipeline will not fail because of formatting errors, we added pre-commit hooks using the `pre-commit` Python library. To use it, simply install it:

<div class="termy">

```console
$ pre-commit install
```

</div>

The pre-commit hooks defined in the [configuration](https://gitlab.eds.aphp.fr/datasciencetools/edsteva/-/blob/master/.pre-commit-config.yaml) will automatically run when you commit your changes, letting you know if something went wrong.

The hooks only run on staged changes. To force-run it on all files, run:

<div class="termy">

```console
$ pre-commit run --all-files
---> 100%
All good !
```

</div>

### 3. Testing your code

We use the Pytest test suite. Writing your own tests is encouraged !

The following command will run the test suite:

<div class="termy">

```console
$ poetry run pytest tests --cov edsteva --junitxml=report.xml
collected X items

tests/test_a.py
tests/test_b.py
tests/test_your_bug.py
---> 100%
```

</div>

Should your contribution propose a bug fix, we require the bug be thoroughly tested.

### 4. Documentation

Make sure to document your improvements, both within the code with comprehensive docstrings,
as well as in the documentation itself if need be.

We use `MkDocs` for EDS-TeVa's documentation. You can checkout the changes you make with:

<div class="termy">

```console
$ mkdocs serve
```

</div>

Go to [`localhost:8000`](http://localhost:8000) to see your changes. MkDocs watches for changes in the documentation folder
and automatically reloads the page.

### 5. Proposing a merge request

At the very least, if your changes are well-documented, pass every tests, and follow the style guide, you can:

- Commit your changes and push your branch:

    <div class="termy">

    ```console
    $ git add *
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature
    ```

    </div>

-  Submit a pull request.
