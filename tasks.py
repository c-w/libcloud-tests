from sys import executable as python

from invoke import task


@task
def setuplint(context):
    dependencies = ["flake8", "pylint", "isort", "black"]
    context.run("{} -m pip install {}".format(python, " ".join(dependencies)))


@task
def lint(context, target="tests tasks.py"):
    context.run("flake8 {}".format(target))
    context.run("pylint {}".format(target))
    context.run("isort --check-only --recursive {}".format(target))
    context.run("black --check {}".format(target))
