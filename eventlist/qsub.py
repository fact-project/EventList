
import os
import subprocess as sp
import pandas as pd

import xmltodict

def build_qsub_command(
        executable,
        stdout = None,
        stderr = None,
        job_name = None,
        queue = None,
        mail_address = None,
        mail_settings = 'a',
        environment = None,
        resources = None,
        engine = 'SGE',
        ):
    """
    Creates a qsub command, with all the different options
    """
    command = []
    command.append('qsub')

    if job_name:
        command.extend(['-N', job_name])

    if queue:
        command.extend(['-q', queue])

    if mail_address:
        command.extend(['-M', mail_address])

    command.extend(['-m', mail_settings])

    # allow a binary executable
    if engine == 'SGE':
        command.extend(['-b', 'yes'])

    if stdout:
        command.extend(['-o', stdout])

    if stderr:
        command.extend(['-e', stderr])

    if environment:
        command.append('-v')
        command.append(','.join(
            '{}={}'.format(k, v)
            for k, v in environment.items()
        ))

    if resources:
        command.append('-l')
        command.append(','.join(
            '{}={}'.format(k, v)
            for k, v in resources.items()
        ))

    command.append(executable)

    return command

def get_current_jobs_PBS(user=None):
    '''
    Return a dataframe with current jobs of the user on the PBS system
    '''
    user = user or os.environ['USER']
    xml = sp.check_output(['qstat', '-x'])
    data = xmltodict.parse(xml)
    arr = data['Data']['Job']
    df = pd.DataFrame(arr)
    df = df[df['Job_Owner'].str.startswith(user)]
    
    cols = ['Job_Name',  'Job_Owner',  'job_state', 'queue', 'ctime', 'start_time']
    df = df[cols]
    df.rename(inplace=True, columns={
        'job_state': 'state',
        'JB_Owner': 'owner',
        'Job_Name': 'name',
        #'JB_job_number': 'job_number',
        'ctime': 'submission_time',
        #'JAT_prio': 'priority',
    })

    df['start_time'] = pd.to_datetime(df['start_time'])
    return df


def get_current_jobs_SGE(user=None):
    '''
    Return a dataframe with current jobs of the user on the SGE system
    '''
    user = user or os.environ['USER']
    xml = sp.check_output(['qstat', '-u', user, '-xml']).decode()
    data = xmltodict.parse(xml)
    job_info = data['job_info']
    queue_info = job_info['queue_info']
    job_info = job_info['job_info']
    queued_jobs = queue_info['job_list'] if queue_info else []
    running_jobs = job_info['job_list'] if job_info else []

    df = pd.DataFrame(columns=[
        '@state', 'JB_job_number', 'JAT_prio', 'JB_name', 'JB_owner',
        'state', 'JB_submission_time', 'queue_name', 'slots', 'JAT_start_time'
    ])

    if not isinstance(running_jobs, list):
        running_jobs = [running_jobs]
    if not isinstance(queued_jobs, list):
        queued_jobs = [queued_jobs]

    df = df.append(pd.DataFrame(running_jobs + queued_jobs), ignore_index=True)

    if len(df) == 0:
        return df

    df.drop('state', axis=1, inplace=True)
    df.rename(inplace=True, columns={
        '@state': 'state',
        'JB_owner': 'owner',
        'JB_name': 'name',
        'JB_job_number': 'job_number',
        'JB_submission_time': 'submission_time',
        'JAT_prio': 'priority',
        'JAT_start_time': 'start_time',
    })

    df = df.astype({'job_number': int, 'priority': float})
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['submission_time'] = pd.to_datetime(df['submission_time'])
    return df

def get_jobs(engine='SGE'):
    """
    Returns the current running jobs
    """
    if engine=='SGE':
        return get_current_jobs_SGE()
    elif engine=='PBS':
        return None
    raise NotImplementedError("Engine "+engine+" not supported")


def create_qsub(file, log_dir, env, kwargs):
    """
    Creates a new qsub to process a single file into the eventlist database
    """
    
    basename = os.path.basename(file)
    
    executable = sp.check_output(
        ['which', 'eventListProcessFile']
    ).decode().strip()
    
    env["FILE"] = file
    command = build_qsub_command(
        executable=executable,
        job_name="eventlist_"+basename,
        environment=env,
        stdout = os.path.join(log_dir, 'eventlist_{}.o'.format(basename)),
        stderr = os.path.join(log_dir, 'eventlist_{}.e'.format(basename)),
        **kwargs,
    )

    return command
