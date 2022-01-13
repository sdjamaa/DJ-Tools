from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from glob import glob
from itertools import groupby
import json
import logging
import logging.config
from operator import itemgetter
import os

from fuzzywuzzy import fuzz
import spotipy
from spotipy.oauth2 import SpotifyOAuth

for logger in ['spotipy', 'urllib3']:
    logger = logging.getLogger(logger)
    logger.setLevel(logging.CRITICAL)

parent = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
log_conf = os.path.join(parent, 'src', 'djtools', 'configs',
                        'logging.conf').replace(os.sep, '/')
logging.config.fileConfig(fname=log_conf, disable_existing_loggers=False,
        defaults={'logfilename': 'move_music_new_structure.log'})
logger = logging.getLogger(__name__)


def get_spotify_tracks(_config, spotify_playlists):
    """Aggregates the tracks from one or more Spotify playlists into a
    dictionary mapped with track title and artist names.

    Args:
        _config (dict): configuration object
        spotify_playlists (dict): playlist names and IDs

    Returns:
        (dict): Spotify tracks keyed by titles and artist names
    """

    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=_config['SPOTIFY_CLIENT_ID'],
            client_secret=_config['SPOTIFY_CLIENT_SECRET'],
            redirect_uri=_config['SPOTIFY_REDIRECT_URI'],
            scope='playlist-modify-public'))
    _tracks = {}
    for playlist, playlist_id in spotify_playlists.items():
        logger.info(f'Getting tracks from Spotify playlist "{playlist}"...')
        __tracks = get_playlist_tracks(spotify, playlist_id, playlist)
        logger.info(f'Got {len(__tracks)} tracks from {playlist}')
        _tracks.update(__tracks)

    logger.info(f'Got {len(_tracks)} tracks in total')

    return _tracks


def get_playlist_tracks(spotify, playlist_id, _playlist):
    """Queries Spotify API for a playlist and pulls tracks from it.

    Args:
        spotify (spotipy.Spotify): Spotify client
        playlist_id (str): playlist ID of Spotify playlist to pull tracks from

    Raises:
        Exception: playlist_id must correspond with a valid Spotify playlist

    Returns:
        set: Spotify track titles and artist names from a given playlist
    """
    try:
        playlist = spotify.playlist(playlist_id)
    except Exception:
        raise Exception(f"Failed to get playlist with ID {playlist_id}") \
                from Exception

    result = playlist['tracks']
    _tracks = add_tracks(result, _playlist)

    while result['next']:
        result = spotify.next(result)
        _tracks.update(add_tracks(result, _playlist))

    return _tracks


def add_tracks(result, playlist):
    """Parses a page of Spotify API result tracks and returns a list of the
    track titles and artist names.

    Args:
        result (spotipy.Tracks): paged result of Spotify tracks

    Returns:
        (list): Spotify track titles and artist names
    """
    _tracks = {}
    for track in result['items']:
        title = track['track']['name']
        artists = ', '.join([y['name'] for y in track['track']['artists']])
        _tracks[f'{title} - {artists}'] = {'added_at': track['added_at'],
                                           'added_by': track['added_by']['id'],
                                           'playlist': playlist}

    return _tracks


def get_beatcloud_tracks():
    """Lists all the music files in S3 and parses out the track titles and
    artist names.

    Returns:
        list: beatcloud track titles and artist names
    """
    logger.info('Getting tracks from the beatcloud...')
    cmd = 'aws s3 ls --recursive s3://dj.beatcloud.com/dj/music/'
    with os.popen(cmd) as proc:
        output = proc.read().split('\n')
    _tracks = [track.split('dj/music/')[-1] for track in output if track]
    logger.info(f'Got {len(_tracks)} tracks')

    return _tracks


def analyze_tracks(_tracks, users):
    logger.info('Analyzing user contributions to spotify playlists...')
    for user_group_id, user_group in groupby(
            sorted(_tracks.values(), key=itemgetter('added_by')),
            key=itemgetter('added_by')):
        logger.info(f'User {users[user_group_id]} ({user_group_id}):')
        for playlist_group_id, playlist_group in groupby(
                sorted(user_group, key=itemgetter('playlist')),
                key=itemgetter('playlist')):
            playlist_group = list(playlist_group)
            logger.info(f'\t{playlist_group_id}: {len(playlist_group)}')


def find_local_files(usb_path, remote_files):
    payload = [[usb_path] * len(remote_files), remote_files]
    with ThreadPoolExecutor(max_workers=os.cpu_count() * 4) as executor:
        _files = list(filter(None,
                             list(executor.map(exists_process, *payload))))
    logger.info(f'Found {len(_files)} local files')

    return _files 


def exists_process(path, _file):
    _path = os.path.join(path, 'DJ Music', _file).replace(os.sep, '/')

    return _file if os.path.exists(_path) else None


def fix_files(bad_files, _local_files, not_test):
    inverse_lookup = {os.path.basename(x): os.path.dirname(x)
            for x in _local_files}
    for bad, good in bad_files.items():
        if not_test:
            os.rename(bad, good)
        else:
            # emulate having renamed bad files (typos, etc.)
            good = os.path.join(inverse_lookup[bad], good).replace(os.sep, '/')
            bad = os.path.join(inverse_lookup[bad], bad).replace(os.sep, '/')
            index = _local_files.index(bad)
            _local_files[index] = good

    return _local_files


def move_local_files(_local_files, _tracks, users, playlist_genres,
                     usb_path, fuzz_ratio, verbosity, ignore, _matches,
                     not_test, cache_fuzz_results):
    # don't consider any files already in the proper username-based folders
    user_names = {os.path.join(x, '').replace(os.sep, '/')
                  for x in users.values()}
    _ = len(_local_files)
    _local_files = [x for x in _local_files
                    if not any((x.startswith(name) for name in user_names))]
    logger.info(f'Ignoring {_ - len(_local_files)} files that are already ' \
                'in the right place')

    # filter local files by those that can directly be mapped to one of the
    # spotify tracks; also remove the corresponding Spotify tracks from future
    # consideration
    found_tracks = {}
    __local_files = []
    for _file in _local_files:
        name = os.path.splitext(os.path.basename(_file))[0]
        track = _tracks.get(name)
        if not track:
            __local_files.append(_file)
            continue
        del _tracks[name]
        found_tracks[name] = track
    _local_files = __local_files
    directly_found = len(found_tracks)
    logger.info(f'Found {directly_found} tracks...fuzzy searching for ' \
                f'the remaining {len(_local_files)}')

    # Load cached fuzzy search results...
    # This tool is intended to be used in iterations with progressively lower
    # minimum 'fuzz_ratio' so as to:
    #    - first fix local files with typos or misformattings (correction
    #      mapping in data['bad_files'])
    #    - then verify that every local track pairs properly with the most
    #      similar Spotify track
    #    - once invalid matches start showing up in the results, make sure each
    #      one is definitely not traceable to a Spotify playlist and then add
    #      to data['ignore']
    #
    # Each iteration should actually be a two-stage process:
    #    (1) see results at newly lowered 'fuzz_ratio', using
    #        `.fuzz_cache.json` to filter previous results, and make any
    #        necessary additions to data['bad_files'] and / or data['ignore']
    #    (2) restore `.fuzz_cache.json` to the backup you made before (1) and
    #        confirm the output at the same 'fuzz_ratio' is as expected...
    #
    # Once all results are definitely not a match at 'fuzz_ratio=0', then
    # you've confirmed every local file that can be attributed to a recognized
    # user of data['users']; what remains outside of the username-based folders
    # came from outside of data['spotify_playlists'] or else is an artifact
    # from a time when the beatcloud contained incorrectly named files that had
    # since been corrected.
    not_matched = []
    if os.path.exists('.fuzz_cache.json'):
        with open('.fuzz_cache.json', encoding='utf-8') as _file:
            cached_fuzz_results = json.load(_file)
        logger.info(f'Ignoring {len(cached_fuzz_results)} fuzz results ' \
                    'previously matched')
        prev_track_count = len(_tracks)
        prev_matches = set(cached_fuzz_results.values())
        _tracks = {k: v for k, v in _tracks.items() if k not in prev_matches}
        logger.info(f'Reduced number of Spotify tracks being considered ' \
                    f'from {prev_track_count} to {len(_tracks)}')
    else:
        cached_fuzz_results = {}

    # distribute fuzzy search of a file against every Spotify track...keep the
    # most similar result above 'fuzz_ratio' and add it to the collection of
    # directly matched files -> tracks
    for _file in _local_files:
        name = os.path.splitext(os.path.basename(_file))[0]
        if any((name in x for x in [cached_fuzz_results, ignore, _matches])):
            continue
        payload = [[name] * len(_tracks),
                    list(_tracks.keys()),
                    [fuzz_ratio] * len(_tracks)]
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 4) as executor:
            __matches = sorted(filter(None, executor.map(fuzz_process,
                                                         *payload)),
                               reverse=True, key=itemgetter(1))
        if not __matches:
            not_matched.append(_file)
            continue
        match, ratio = __matches[0]
        found_tracks[name] = _tracks[match]
        found_tracks[name]['fuzz_ratio'] = ratio
        found_tracks[name]['match'] = match
        # del _tracks[match]
        if cache_fuzz_results:
            cached_fuzz_results[name] = match

    if cache_fuzz_results:
        with open('.fuzz_cache.json', 'w', encoding='utf-8') as _file:
            json.dump(cached_fuzz_results, _file)

    # optionally display all the results of the fuzzy search
    if verbosity > 0:
        logger.info(f'Fuzzy matched files:')
        for name, track in found_tracks.items():
            if not track.get('fuzz_ratio'):
                continue
            logger.info(f"\t{track['fuzz_ratio']}: {name}")
            logger.info(f"\t    {track['match']}")
    logger.info(f'Fuzzy matched {len(found_tracks) - directly_found} files')
    logger.info(f"Unable to find {len(not_matched)} tracks (plus the " \
                f"{len(ignore)} in data['ignore'])")

    # move every local file matched with a playlist and user to that user's
    # corresponding data['playlist_genres'] folder
    for name, track in found_tracks.items():
        user = users[track['added_by']]
        genre = playlist_genres[track['playlist']]
        dest = os.path.join(usb_path, 'DJ Music', user, genre, 'old')
        if not os.path.exists(dest):
            if not_test:
                make_dirs(dest)
        if not_test:
            os.rename(name, os.path.join(dest, name).replace(os.sep, '/'))


def fuzz_process(file_name, track_name, threshold):
    fuzz_ratio = fuzz.ratio(file_name.lower(), track_name.lower()) 
    if fuzz_ratio >= threshold:
        return (track_name, fuzz_ratio)


def make_dirs(dest):
    if os.name == 'nt':
        cwd = os.getcwd()
        path_parts = dest.split(os.path.sep)
        root = path_parts[0]
        path_parts = path_parts[1:]
        os.chdir(root)
        for part in path_parts:
            os.makedirs(part, exist_ok=True)
            os.chdir(part)
        os.chdir(cwd)
    else:
        os.makedirs(dest, exist_ok=True)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--structure_data', required=True,
            help='JSON with keys:\n' \
                 '\t"spotify_playlists: map playlist_name -> playlist_id"\n' \
                 '\t"playlist_genres: "\n' \
                 '\t"users"\n' \
                 '\t"bad_files"\n' \
                 '\t"ignore"')
    parser.add_argument('--not_test', action='store_true',
            help="actual execute file moves")
    parser.add_argument('--config_path', help='path to config.json')
    parser.add_argument('--fuzz_ratio', default=80, type=float,
            help='fuzz ratio')
    parser.add_argument('--move_remote_files', action='store_true',
            help="rename files in S3 so they're under the proper user and " \
                 "genre folders")
    parser.add_argument('--cache_fuzz_results', action='store_true',
            help="cache fuzz matches that are supposedly correct...used to " \
                 "clean up output as fuzz ratio is progressively lowered")
    parser.add_argument('--verbosity', '-v', action='count', default=0,
            help='logging verbosity')
    args = parser.parse_args()

    if not os.path.exists(args.structure_data):
        raise Exception(f"required `--structure_data` JSON config " \
                        f"'{args.structure_data}' doesn't exist")

    data = json.load(open(args.structure_data, encoding='utf-8'))

    # validate all bad_file lookups have .mp3 extensions
    for key, value in data['bad_files'].items():
        if any((not x.endswith('.mp3') for x in [key, value])):
            raise ValueError(f'"{key, value}" must end with ".mp3"')

    # temporary buffer to hold local file results for special consideration
    # while fuzzy searching
    _matches = {
    }

    # standard djtools 'config.json' file (for USB_PATH, AWS_PROFILE, etc.)
    config = json.load(open(args.config_path)) 

    # cached Spotify API results and `aws s3 ls --recursive` on 'dj/music/'
    cache_path = os.path.join(os.path.dirname(__file__),
                              '.cache.json').replace(os.sep, '/')

    if not os.path.exists(cache_path):
        tracks = get_spotify_tracks(config, data['spotify_playlists'])
        files = get_beatcloud_tracks()
        with open(cache_path, 'w', encoding='utf-8') as _file:
            json.dump({'tracks': tracks, 'files': files}, _file)
    else:
        with open(cache_path, encoding='utf-8') as _file:
            cache = json.load(_file)
            tracks = cache['tracks']
            files = cache['files']
        logger.info(f'Retrieved {len(tracks)} tracks and {len(files)} files ' \
                    'from cache.')

    # display data['users'] contributions to data['spotify_playlists']
    analyze_tracks(tracks, data['users'])

    # filter `aws s3 ls` result for those that exist at config['USB_PATH']
    local_files = find_local_files(config['USB_PATH'], files)

    # optionally display the beatcloud files that aren't present locally
    if args.verbosity > 0:
        unfound = set(files).difference(set(local_files))
        logger.info(f'{len(unfound)} files in S3 not found locally:')
        for _file in unfound:
            logger.info(f'\t{_file}')

    # rename files (only if `--not_test`), otherwise swaps 'bad' local files in
    # index for the 'good' ones mapped in data['bad_files']
    local_files = fix_files(data['bad_files'], local_files, args.not_test)

    # This operation is meant to be run multiple times with progressively lower
    # 'fuzz_ratio'...each time:
    #    - typos and misformattings should be corrected using data['bad_files']
    #    - correct matches should be allowed to cache in `.fuzz_cache.json` 
    #    - incorrect matches must be confirmed unattributable to a user and
    #      added to data['ignore']; incorrect matches that are permitted may
    #      result in a track being moved to a technically incorrect user /
    #      genre folder
    logger.info('Moving local files...')
    data['ignore'] = set(data['ignore'])
    move_local_files(local_files, tracks, data['users'],
                     data['playlist_genres'], config['USB_PATH'],
                     args.fuzz_ratio, args.verbosity, data['ignore'],
                     _matches, args.not_test, args.cache_fuzz_results)
