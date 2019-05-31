from invoke import task


@task
def lint(context, target="tests tasks.py"):
    context.run("flake8 {}".format(target))
    context.run("pylint {}".format(target))
    context.run("isort --check-only --recursive {}".format(target))
    context.run("black --check {}".format(target))
