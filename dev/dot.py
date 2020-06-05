from osa.utils import options
from osa.utils.standardhandle import warning, verbose, error, gettag


def writeworkflow(sequence_list):
    tag = gettag()
    from dev.iofile import writetofile
    from os.path import join, exists
    from osa.configs.config import cfg
    from osa.utils.utils import lstdate_to_number
    replaced = None
    dot_basename = "{0}_{1}_{2}{3}".format(
        cfg.get('LSTOSA', 'WORKFLOWPREFIX'),
        lstdate_to_number(options.date),
        options.tel_id, cfg.get('LSTOSA', 'GRAPHSUFFIX')
    )
    dot_path = join(options.directory, dot_basename)

    # We could think of using the pydot interface as well, but this is relatively simple anyway
    content = "strict digraph {\n"
    content += "label=\"LSTOSA workflow for " + options.tel_id + " on " + options.date + "\";"
    content += "labelloc=t;\n"
    content += "rankdir=LR;\n"
    content += "node [shape=box];\n"
    content += "edge [headport=w];\n"
    content += "start [label=\"\", style=invisible];\n"
    # Now we reversely-loop over the requirements (just for make ordered the drawing in Left-Right mode)
    for i in sequence_list:
        content += 'n{1} [label="{0} {1}|{2} [{3}]"\nshape="record"];\n'.format(i.telescope, i.seq, i.run, i.subruns)
        if len(i.parent_list) == 0:
            content += "start -> n{0} [style=invis];\n".format(i.seq)
        elif len(i.parent_list) == 1:
            """ One parent, typical single process mode, id different"""
            content += "n{0} -> n{1};\n".format(i.parent_list[0].seq, i.seq)
        else:
            """ Two parents, stereo mode, assign indexes for each range,
            this means 0 - m-1 (for the st), m - 2m-1 (for LST1), 2m - 3m-1 (for LST2)"""
            m = len(sequence_list)
            index = i.seq
            for j in i.parent_list:
                index += m
                content += 'n{0} [label="{1} {2}|{3} [{4}]"\nshape="record"];\n'.format(index, j.telescope, j.seq,
                                                                                        j.run, j.subruns)
                content += "start -> n{0} [style=invis];\n".format(index)
                content += "n{0} -> n{1};\n".format(index, i.seq)
    # Closing the content
    content += "}\n"
    if not options.simulate:
        replaced = writetofile(dot_path, content)
    verbose(tag, "Workflow updated? {0} in {1}".format(replaced, dot_path))
    svg_path = dot_path.rsplit('.', 1)[0] + cfg.get('LSTOSA', 'SVGSUFFIX')
    if replaced or not exists(svg_path):
        verbose(tag, "Updating workflow file: {0}".format(dot_path))
        convert_dot_into_svg(dot_path, svg_path)


def convert_dot_into_svg(dotfile, svgfile):
    tag = gettag()
    import subprocess
    from osa.configs.config import cfg

    command = cfg.get('LSTOSA', 'GRAPH')
    svgflag = '-' + cfg.get('LSTOSA', 'SVGSUFFIX').replace('.', 'T')
    try:
        commandoutput = subprocess.check_output(['which', command])
    except subprocess.CalledProcessError as Error:
        error(tag, Error, 3)
    else:
        commandargs = [command, svgflag, '-o' + svgfile, dotfile]

    try:
        subprocess.call(commandargs)
    # except OSError as (ValueError, NameError):
    except OSError as NameError:
        warning(tag, "svg file could not be created from dot file {0}, {1}".format(dotfile, NameError))
    else:
        verbose(tag, "Workflow sketched in file {0} ".format(dotfile))
