"""Deployment-scoped public collection access; ROS and management auth are separate."""
import copy
import os
import re

from .store import Principal,ident


_ID = r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}'
_ROUTES = (
    (re.compile(r'^/(?:healthz|openapi\.json|collection/status)$'), ('GET','HEAD')),
    (re.compile(r'^/collection/(?:start|redo|complete|finish|sessions|abandon)$'), ('POST',)),
    (re.compile(r'^/collection/operations/'+_ID+r'$'), ('GET','HEAD')),
    (re.compile(r'^/collection/sessions/'+_ID+r'/(?:stop|resume|finish)$'), ('POST',)),
    (re.compile(r'^/collection/sessions/'+_ID+r'/episodes/'+_ID+r'/(?:redo|complete)$'), ('POST',)),
    # 只读视频：画面来源列表、单帧 JPEG、WebSocket 推流
    (re.compile(r'^/collection/video(?:/ws|/[a-z][a-z0-9_]{0,31}\.jpg)?$'), ('GET','HEAD')),
)


class CollectionAccess:
    def __init__(self, platform, accounts):
        self.enabled=False;self.principal=None
        if platform.role!='collection':return
        cfg=getattr(platform,'config',{})
        mode=cfg.get('collection_rest_auth') or os.environ.get('SO101_COLLECTION_REST_AUTH') or 'bearer'
        if mode not in ('none','bearer'):raise ValueError('collection_rest_auth must be none or bearer')
        if mode!='none':return
        project=cfg.get('collection_project') or os.environ.get('SO101_COLLECTION_PROJECT')
        name=cfg.get('collection_rest_operator') or os.environ.get('SO101_COLLECTION_REST_OPERATOR')
        candidates=[a for a in accounts if 'operator' in a.get('roles',()) and (not name or a.get('name')==name)]
        if not project:
            choices={p for a in candidates for p in a.get('projects',())}
            if len(choices)!=1:raise ValueError('Public collection requires a fixed deployment project')
            project=choices.pop()
        ident(project)
        candidates=[a for a in candidates if project in a.get('projects',())]
        if len(candidates)!=1:raise ValueError('Public collection requires one explicitly bound existing operator')
        ident(candidates[0]['name'])
        self.principal=Principal(candidates[0]['name'],(project,),('operator',))
        self.enabled=True

    def methods(self,path):
        if not self.enabled:return ()
        for pattern,methods in _ROUTES:
            if pattern.fullmatch(path):return methods
        return ()

    def openapi(self,document):
        if not self.enabled:return document
        result=copy.deepcopy(document)
        for path,route in result.get('paths',{}).items():
            actual=re.sub(r'\{[^{}]+\}','id',path)
            methods=self.methods(actual)
            for verb,operation in route.items():
                if verb.upper() in methods and isinstance(operation,dict):operation['security']=[]
        result['info']['x-collection-rest-access']={'mode':'none','scope':'collection business routes',
            'operator':self.principal.name,'project':self.principal.projects[0],
            'browser_credentials':False,'ros_authentication':'unchanged'}
        return result
