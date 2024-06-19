"""
This script is a companion to the 
  "Creating and Executing Batch Changes via the Sourcegraph GraphQL API" blog post.
It is intended to be an example of how to call the steps outlined in the post and
  not production quality code.
"""

import argparse
import logging
import os
import sys
import time
import yaml
import requests

def main() -> int:
    """
    main interface to this script
    """

    parser = argparse.ArgumentParser(description='Sourcegraph Batch Change Creator')
    parser.add_argument('--namespace', '-n', type=str, required=True,
                        help='The owning namespace (either a username or a organization)')
    parser.add_argument('--yaml', '-y', type=str, required=True,
                        help='The file representing the batch change specification')
    parser.add_argument('--sg-url', '-u', type=str, required=False,
                        help='The URL to the Sourcegraph server (default is env SRC_ENDPOINT)')
    parser.add_argument('--sg-access-token', '-a', type=str, required=False,
                        help='The Sourcegraph user token (default is env SRC_ACCESS_TOKEN)')
    parser.add_argument('--sg-skip-cert-check', '-k', action='store_true', required=False,
                        help='Skip validating the SSL cert when communicating with SG server?')
    parser.add_argument('--verbose', '-v', action='store_true', required=False,
                        help='Verbose logging')
    parser.add_argument('--timeout', '-t', type=int, required=False, default=60,
                        help='Timeout for all waiting operations in seconds')

    # Parse the command line arguments
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG if args.verbose else logging.WARNING)
    logger = logging.getLogger(__name__)

    if not args.sg_url:
        if not 'SRC_ENDPOINT' in os.environ:
            print('Provide a value for --sg-url or set the environment variable SRC_ENDPOINT')
            return 1
        url = os.environ['SRC_ENDPOINT']
    else:
        url = args.sg_url

    if not args.sg_access_token:
        if not 'SRC_ACCESS_TOKEN' in os.environ:
            print('Provide a value for --sg-access-token or set the environment variable SRC_ACCESS_TOKEN')
            return 1
        token = os.environ['SRC_ACCESS_TOKEN']
    else:
        token = args.sg_url
    sgapi = Sourcegraph(url, token, not args.sg_skip_cert_check)

    try:
        # parse the yaml for the name since it's needed for the call below
        with open(args.yaml, 'r') as yamlfd:
            data = yaml.safe_load(yamlfd)
            bc_name = data['name']

        # step 1
        namespace_id = sgapi.get_namespace_id(args.namespace)
        print(f'namespace ID for "{args.namespace}" is "{namespace_id}"')
        # step 2
        batch_change_id = sgapi.create_batch_change(namespace_id, bc_name)
        print(f'Batch Change created with ID "{batch_change_id}"')
        # step 3
        spec_contents = get_spec_contents_as_string(args.yaml)
        # step 4
        batch_spec_id = sgapi.upload_batch_spec(namespace_id, batch_change_id, spec_contents)
        print(f'Batch Spec created with ID "{batch_spec_id}"')
        sgapi.wait_for_batch_spec_state(batch_spec_id,
                                        [ 'COMPLETED' ],
                                        [ 'PENDING', 'QUEUED' ],
                                        args.timeout)

        # step 5
        print('Executing Batch Change ...')
        sgapi.execute_batch_spec(batch_spec_id)
        # step 6
        print('Waiting for Batch Change to be complete ...')
        sgapi.wait_for_batch_spec_state(batch_spec_id,
                                        [ 'COMPLETED' ],
                                        [ 'COMPLETED' ],
                                        args.timeout)
        # step 7
        print('Creating PRs ...')
        sgapi.apply_batch_change(batch_spec_id)
        # step 8
        changeset_ids = sgapi.wait_all_batch_change_changesets_state(batch_change_id,
                                                                     [ 'COMPLETED' ],
                                                                     args.timeout)

        # step 9
        print(f'Publishing PRs ({changeset_ids}) to code host(s) ...')
        errs, state = sgapi.publish_batch_change_changesets(batch_change_id, changeset_ids)
        if errs:
            print(f'Failed to publish some changesets: {errs}')
        print(f'Publish: {state}')

    except Exception as exc:
        if args.verbose:
            logger.exception('Failed')
        else:
            logger.error(str(exc))
        return 1

    return 0

class Sourcegraph():
    """
    class that handles interaction with Sourcegraph api
    """

    def __init__(self, url: str, token: str, verify_ssl_cert: bool):
        """
        constructor

        :param url: the sourcegraph URL
        :param token: the sourcegraph API access token
        :param verify_ssl_cert: verify SSL cert of Sourcegraph server when connecting?
        :raises ValueError: when url or token is not set
        """

        if not url:
            raise ValueError('Sourcegraph URL must be set')
        if not token:
            raise ValueError('Sourcegraph API token must be set')

        # generate the full URL to the Sourcegraph GraphQL API endpoint
        if url[-1] != '/':
            self.endpoint = url + '/.api/graphql'
        else:
            self.endpoint = url + '.api/graphql'

        self.api_token = token
        self.verify_ssl_cert = verify_ssl_cert
        self.logger = logging.getLogger(self.__class__.__name__)

    def api(self, query: str, variables: dict) -> dict:
        """
        execute the given query on sourcegraph API endpoint
        :param query: the GraphQL query or mutation string
        :param variables: the GraphQL query/mutation variables dictionary
        :raises ValueError: when there is an error
        :return: the response from the server
        """

        data = {'query': query}
        if variables:
            data['variables'] = variables

        headers = {"Authorization": f"token {self.api_token}"}
        self.logger.debug('Calling GraphQL API on %s\n%s\nVariables:  %s',
                          self.endpoint, query, variables)
        response = requests.post(self.endpoint,
                                 json=data,
                                 headers=headers,
                                 timeout=10,
                                 verify=self.verify_ssl_cert)
        self.logger.debug('Response code: %d', response.status_code)
        if response.status_code != 200:
            self.logger.warning('%s [%s]', response.text, self.endpoint)
            raise ValueError(response.text)

        response_json = response.json()
        if 'errors' in response_json:
            if len(response_json["errors"]) == 1:
                msg = response_json['errors'][0]['message']
            else:
                msg = str(response_json['errors'])
            msg = msg + f' [{self.endpoint}]'
            self.logger.warning(msg)
            raise ValueError(msg)
        if 'data' in response_json:
            self.logger.debug('Response: %s', str(response_json['data']))
            return response_json['data']
        self.logger.debug('Response: %s', str(response_json))
        return response_json

    def get_namespace_id(self, name: str) -> str:
        """
        Gets the Sourcegraph Namespace ID for the given namespace name.
        :param name: the user name or organization name
        :raises ValueError: when the name is not found
        :return: The Namespace ID for the given name
        """

        query = """
query NamespaceByName($name: String!) {
  namespaceByName(name: $name) {
    id
  }
}
"""

        response = self.api(query, {'name': name})['namespaceByName']
        if response:
            return response['id']
        raise ValueError(f'"{name}" not found!')

    def create_batch_change(self, namespace_id: str, bc_name: str) -> str:
        """
        Creates an empty batch change object
        :param namespace_id: the namespace ID returned from get_namespace_id()
        :param bc_name: the name of the batch change
        :return: the ID of the batch change object created
        """

        mutation = """
mutation CreateEmptyBatchChange($NamespaceID: ID!, $name: String!) {
  createEmptyBatchChange(namespace: $NamespaceID, name: $name) {
    id
    url
  }
}
"""
        variables = {'NamespaceID': namespace_id, 'name': bc_name}
        response = self.api(mutation, variables)['createEmptyBatchChange']
        return response['id']

    def upload_batch_spec(self, namespace_id: str, bc_id: str, spec_contents: str) -> str:
        """
        Uploads the batch change specification file
        :param namespace_id: the namespace ID returned from get_namespace_id()
        :param bc_id: the ID of the batch change (from create_batch_change())
        :param spec_contents: the contents of the yaml specification file
        :return: the ID of the batch change object created
        """

        mutation = """
mutation CreateBatchSpecFromRaw($batchSpec: String!, $NamespaceID: ID!, $BatchChangeID: ID!) {
  createBatchSpecFromRaw(batchSpec: $batchSpec, namespace: $NamespaceID, batchChange: $BatchChangeID) {
    id
    state
  }
}
"""

        variables = {'batchSpec': spec_contents,
                     'BatchChangeID': bc_id,
                     'NamespaceID': namespace_id}
        response = self.api(mutation, variables)['createBatchSpecFromRaw']
        return response['id']

    def execute_batch_spec(self, batch_spec_id: str):
        """
        executes the given batch spec and verifies the state is QUEUED or COMPLETED
        :param batch_spec_id: the batch spec id
        """

        mutation = """
mutation ExecuteBatchSpec($BatchSpecID: ID!) {
  executeBatchSpec(batchSpec: $BatchSpecID) {
    state
    id
    applyURL
  }
}
"""

        variables = {'BatchSpecID': batch_spec_id}
        response = self.api(mutation, variables)['executeBatchSpec']
        if response['state'] not in [ 'QUEUED', 'COMPLETED' ]:
            raise ValueError('Unable to execute batch spec "{batch_spec_id}" (state is {response["state"]})')

    def get_batch_change_workspace_resolution_status(self, batch_spec_id: str):
        """
        Gets the workspace resolution status for this batch spec
        :param batch_spec_id: the batch spec ID
        :return: tuple (wsr_state, overall_state)
        :rtype: tuple
        """

        query = """
query WorkspaceResolutionStatus($BatchSpecID: ID!) {
  node(id: $BatchSpecID) {
    ... on BatchSpec {
          workspaceResolution {
            startedAt
            state
            failureMessage          
    }
    id
    state
     }
  }
}
"""

        response = self.api(query, {'BatchSpecID': batch_spec_id})['node']
        return response['workspaceResolution']['state'], response['state']

    def get_batch_change_changesets(self, batch_change_id: str, start_cursor: str):
        """
        gets the list of associated changeset IDs
        :param batch_chagne_id: the batch change ID
        :param start_cursor: the starting point (None for 0)
        :return: tuple:  list of changeset objects {'id', 'state'}, next_page_cursor
        :rtype: tuple
        """

        query = """
query BatchChangeChangesets($BatchChangeID: ID!, $after: String) {
  node(id: $BatchChangeID) {
    ... on BatchChange {
      changesets(after: $after) {
        totalCount,
        nodes {
          id
          state
        }
        pageInfo {
          endCursor
          hasNextPage
        }
      }
      url
      state
    }
  }
}
"""

        variables = {'BatchChangeID': batch_change_id, 'after': start_cursor}
        response = self.api(query, variables)['node']['changesets']
        return response['nodes'], response['pageInfo']['endCursor']

    def get_all_batch_change_changesets(self, batch_change_id: str) -> list:
        """
        gets the list of all assocaited changeset IDs
        :param batch_change_id: the batch change ID
        :return: list of changeset objects {'id', 'state'}
        :rtype: list
        """

        start_cursor = None
        all_ids = []
        while True:
            ids, start_cursor = self.get_batch_change_changesets(batch_change_id, start_cursor)
            all_ids.extend(ids)
            if not start_cursor:
                break
        return all_ids

    def wait_all_batch_change_changesets_state(self, batch_change_id: str, states: list,
                                               timeout_sec: int) -> list:
        """
        waits for all batch change changesets to be in the given state(s)
        :param batch_chnge_id: the batch change ID
        :param states: list of allowed states
        :param timeout_sec: maximum number of seconds to wait
        :return: list of changeset ids
        :rtype: list
        """

        timeout_at = time.time() + timeout_sec
        while True:
            changesets = self.get_all_batch_change_changesets(batch_change_id)
            for changeset in changesets:
                if changeset['state'] not in states:
                    if time.time() > timeout_at:
                        raise TimeoutError()
                    time.sleep(1)
                    continue
            # if we get here then all changesets are in an allowed state
            return [changeset['id'] for changeset in changesets]

    def wait_for_batch_spec_state(self, batch_spec_id: str, wsr_states: list,
                                  states: list, timeout_sec: int):
        """
        waits for the given batch spec state to be in one of `states` (or timeout)
        :param batch_spec_id: the batch spec ID to check
        :param wsr_states: list of strings representing WSR states that are acceptable
        :param states: list of strings representing states that are acceptable
        :param timeout_sec: maximum number of seconds to wait
        :raises TimeoutError: if still waiting after timeout_sec
        """

        wsr_state = ''
        state = ''
        timeout_at = time.time() + timeout_sec
        while True:
            wsr_state, state = self.get_batch_change_workspace_resolution_status(batch_spec_id)
            self.logger.info('WSR state: %s, state: %s', wsr_state, state)
            if wsr_state in wsr_states and state in states:
                break
            if time.time() > timeout_at:
                raise TimeoutError()
            time.sleep(1)

    def apply_batch_change(self, batch_spec_id: str):
        """
        applies the given batch spec
        :param batch_spec_id: the batch spec id
        :raises ValueError: when the batch change cannot be applied
        """

        mutation = """
mutation ApplyBatchChange($BatchSpecID: ID!) {
  applyBatchChange(batchSpec: $BatchSpecID) {
    id,
    name,
    state,
    url
  }
}
"""

        variables = {'BatchSpecID': batch_spec_id}
        response = self.api(mutation, variables)['applyBatchChange']
        if response['state'] not in [ 'OPEN' ]:
            raise ValueError('Unable to execute batch spec "{batch_spec_id}" (state is {response["state"]})')

    def publish_batch_change_changesets(self, batch_change_id: str, changeset_ids: list):
        """
        Publish the PRs to the code host(s)
        :param batch_change_id: the batch change ID
        :param changeset_ids: list of changeset IDs
        :return: tuple: error_changeset_ids, state
        :rtype: tuple
        """

        mutation = """
mutation PublishChangesets($BatchChangeID: ID!, $changesets: [ID!]!, $draft: Boolean) {
  publishChangesets(batchChange: $BatchChangeID, changesets: $changesets, draft: $draft) {
    errors {
      changeset {
        id
        state
      }
      error
    }
    changesetCount
    progress
    state
    finishedAt
  }
}
"""
        variables = {'BatchChangeID': batch_change_id,
                     'changesets': changeset_ids,
                     'draft': True}
        response = self.api(mutation, variables)['publishChangesets']
        return response['errors'], response['state']

def get_spec_contents_as_string(yaml_path: str) -> str:
    """
    Gets the batch changes specification from the file path provided and returns
    a single line string representing the contents
    :param yaml_path: the path to the Batch Changes YAML specification file
    :return: a single line string representation of the contents of the file
    """

    with open(yaml_path, 'r') as yamlfd:
        return yamlfd.read()

if __name__ == '__main__':
    main()
