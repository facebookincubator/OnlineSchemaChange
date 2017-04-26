# Contributing to OnlineSchemaChange
We want to make contributing to this project as easy and transparent as
possible.

## Our Development Process
Once you submitted a diff, our github robot will sync it into our internal
repo to run all unittest and integration test suite making sure the change
won't cause regression and bugs. In the meantime, our team will review the diff
to make sure it meets our coding style and properly designed.

Once approved the diff will be commited to our internal repo and sync back to
github. Afterwards you'll be able to see your changes on GitHub!

## Contributor License Agreement ("CLA")
In order to accept your pull request, we need you to submit a CLA. You only need
to do this once to work on any of Facebook's open source projects.

Complete your CLA here: <https://code.facebook.com/cla>

## Pull Requests
We actively welcome your pull requests.

1. Fork the repo and create your branch from `master`.
2. If you've added code that should be tested, add both unit and integration tests.
3. If you've changed behaviour, make sure you have proper discussion with us through [slack](https://onlineschemachange.slack.com/) before
4. Ensure the test suite passes.
5. Make sure your code lints.
6. If you haven't already, complete the Contributor License Agreement ("CLA").

Any code changes for adding features or fixing bugs, we suggest having both unittest case and integration test cases implemented and log outputs reflecting that the changes involved have been tested and works in the pull request. 

Unit tests should be put under `osc/tests` with proper naming. 
Integration tests should be put under `osc/tests/integration`

## Coding Style  
* 4 spaces for indentation rather than tabs
* 80 character line length
* Make sure your change can pass the [`flake8`](http://flake8.pycqa.org/en/latest/)'s check.

## Feature Requests
If you find some feature is missing from `osc_cli` which prevent you from putting it into your production environment. Feel free to submit a feature request. Please check known [[Limitation|Limitation]] and existing requests first before firing one.

## Issues
We use GitHub issues to track public bugs. 
If you find OSC fails to run a schema change for you, feel free to fire a bug report. We encourage you to provide as much as information as possible. Below are the details that will help us investigate and reproduce the issue:

* MySQL Version, Python Version
* MySQL Running configure if relavant
* Original schema and desired schema (subs out sensitive informations if necessary)
* `osc_cli` options used 
* Log output from the failed `osc_cli` attempt 

If you could provide an integration test case that can reproduce the issue, that's even greater. This way we can make sure the patch actually fix the reported issue. See  for [[Write an Integration Test Case|Write-an-Integration-Test-Case]]
more detail
## License
By contributing to OnlineSchemaChange, you agree that your contributions will 
be licensed under the LICENSE file in the root directory of this source tree.
